package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.MemoryUsageProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisAPIProvider;

import java.util.Arrays;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class EnqueueAction extends AbstractQueueAction {

    private final MemoryUsageProvider memoryUsageProvider;
    private final int memoryUsageLimitPercent;

    public EnqueueAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPIProvider redisAPIProvider, String address, String queuesKey, String queuesPrefix,
                         String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                         QueueStatisticsCollector queueStatisticsCollector, Logger log, MemoryUsageProvider memoryUsageProvider, int memoryUsageLimitPercent) {
        super(vertx, luaScriptManager, redisAPIProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
        this.memoryUsageProvider = memoryUsageProvider;
        this.memoryUsageLimitPercent = memoryUsageLimitPercent;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);

        if (isMemoryUsageLimitReached()) {
            log.warn("Failed to enqueue into queue {} because the memory usage limit is reached", queueName);
            event.reply(createErrorReply().put(MESSAGE, MEMORY_FULL));
            return;
        }
        updateTimestamp(queueName).onComplete(updateTimestampEvent -> {
            if (updateTimestampEvent.failed()) {
                replyError(event, queueName, updateTimestampEvent.cause());
                return;
            }
            String keyEnqueue = queuesPrefix + queueName;
            String valueEnqueue = event.body().getString(MESSAGE);

            redisAPIProvider.redisAPI().onSuccess(redisAPI -> redisAPI.rpush(Arrays.asList(keyEnqueue, valueEnqueue)).onComplete(enqueueEvent -> {
                JsonObject reply = new JsonObject();
                if (enqueueEvent.succeeded()) {
                    if (log.isDebugEnabled()) {
                        log.debug("RedisQues Enqueued message into queue {}", queueName);
                    }
                    long queueLength = enqueueEvent.result().toLong();
                    notifyConsumer(queueName);
                    reply.put(STATUS, OK);
                    reply.put(MESSAGE, "enqueued");

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
                } else {
                    replyError(event, queueName, enqueueEvent.cause());
                }
            }));
        });
    }

    private void replyError(Message<JsonObject> event, String queueName, Throwable cause) {
        String message = "RedisQues QUEUE_ERROR: Error while enqueueing message into queue " + queueName;
        log.error(message, cause);
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
