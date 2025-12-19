package org.swisspush.redisques.action;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.QueueRegistryService;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.*;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class EnqueueAction extends AbstractQueueAction {

    private final MemoryUsageProvider memoryUsageProvider;
    private final QueueRegistryService queueRegistryService;
    private final int memoryUsageLimitPercent;
    private Counter enqueueCounterSuccess;
    private Counter enqueueCounterFail;
    public EnqueueAction(
            Vertx vertx, QueueRegistryService queueRegistryService, RedisService redisService, KeyspaceHelper keyspaceHelper, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log,
            MemoryUsageProvider memoryUsageProvider, int memoryUsageLimitPercent, MeterRegistry meterRegistry, String metricsIdentifier
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
        this.memoryUsageProvider = memoryUsageProvider;
        this.memoryUsageLimitPercent = memoryUsageLimitPercent;
        this.queueRegistryService = queueRegistryService;

        if (meterRegistry != null) {
            enqueueCounterSuccess = Counter.builder(MetricMeter.ENQUEUE_SUCCESS.getId()).description(MetricMeter.ENQUEUE_SUCCESS.getDescription())
                    .tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).register(meterRegistry);
            enqueueCounterFail = Counter.builder(MetricMeter.ENQUEUE_FAIL.getId()).description(MetricMeter.ENQUEUE_FAIL.getDescription())
                    .tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).register(meterRegistry);
        }
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);

        if (isMemoryUsageLimitReached()) {
            log.warn("Failed to enqueue into queue {} because the memory usage limit is reached", queueName);
            incrEnqueueFailCount();
            event.reply(createErrorReply().put(MESSAGE, MEMORY_FULL));
            return;
        }
        queueRegistryService.updateTimestamp(queueName, true, updateTimestampEvent -> {
            if (updateTimestampEvent.failed()) {
                replyError(event, queueName, updateTimestampEvent.cause());
                return;
            }
            String keyEnqueue = keyspaceHelper.getQueuesPrefix() + queueName;
            String valueEnqueue = event.body().getString(MESSAGE);

            redisService.rpush(keyEnqueue, valueEnqueue).onComplete(enqueueEvent -> {
                JsonObject reply = new JsonObject();
                if (enqueueEvent.succeeded()) {
                    if (log.isDebugEnabled()) {
                        log.debug("RedisQues Enqueued message into queue {}", queueName);
                    }
                    processTrimRequestByState(queueName).onComplete(asyncResult -> {
                        if (asyncResult.failed()) {
                            log.warn("Failed to do the trim for  {}", queueName);
                        }
                        long queueLength = enqueueEvent.result().toLong();
                        queueRegistryService.notifyConsumer(queueName).onComplete(asyncResult1 -> {
                            if (asyncResult1.failed()) {
                                replyError(event, queueName, enqueueEvent.cause());
                                return;
                            }
                            reply.put(STATUS, OK);
                            reply.put(MESSAGE, "enqueued");

                            incrEnqueueSuccessCount();

                            // feature EN-queue slow-down (the larger the queue the longer we delay "OK" response)
                            long delayReplyMillis = 0;
                            QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
                            if (queueConfiguration != null) {
                                float enqueueDelayFactorMillis = queueConfiguration.getEnqueueDelayFactorMillis();
                                if (enqueueDelayFactorMillis > 0f) {
                                    // minus one as we need the queueLength _before_ our en-queue here
                                    delayReplyMillis = (long) ((queueLength - 1) * enqueueDelayFactorMillis);
                                    int max = queueConfiguration.getEnqueueMaxDelayMillis();
                                    if (max > 0 && delayReplyMillis > max) {
                                        delayReplyMillis = max;
                                    }
                                }
                            }
                            if (delayReplyMillis > 0) {
                                vertx.setTimer(delayReplyMillis, timeIsUp -> event.reply(reply));
                            } else {
                                event.reply(reply);
                            }
                            queueStatisticsCollector.setQueueBackPressureTime(queueName, delayReplyMillis);
                        });
                    });
                } else {
                    replyError(event, queueName, enqueueEvent.cause());
                }
            });
        });
    }

    private void incrEnqueueSuccessCount() {
        if (enqueueCounterSuccess != null) {
            enqueueCounterSuccess.increment();
        }
    }

    protected void incrEnqueueFailCount() {
        if (enqueueCounterFail != null) {
            enqueueCounterFail.increment();
        }
    }

    private void replyError(Message<JsonObject> event, String queueName, Throwable ex) {
        incrEnqueueFailCount();
        String message = "RedisQues QUEUE_ERROR: Error while enqueueing message into queue " + queueName;
        log.error(message, new Exception(ex));
        JsonObject reply = new JsonObject();
        reply.put(STATUS, ERROR);
        reply.put(MESSAGE, message);
        event.reply(reply);
    }

    protected boolean isMemoryUsageLimitReached() {
        if (memoryUsageProvider.currentMemoryUsagePercentage().isEmpty()) {
            return false;
        }
        return memoryUsageProvider.currentMemoryUsagePercentage().get() > memoryUsageLimitPercent;
    }
}
