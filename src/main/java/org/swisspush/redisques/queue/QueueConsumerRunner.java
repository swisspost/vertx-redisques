package org.swisspush.redisques.queue;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.QueueStatsService;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisQuesTimer;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

public class QueueConsumerRunner {
    private static final Logger log = LoggerFactory.getLogger(QueueConsumerRunner.class);
    private final Vertx vertx;
    private final RedisService redisService;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final QueueMetrics metrics;
    private final QueueStatsService queueStatsService;
    private final KeyspaceHelper keyspaceHelper;
    private final RedisquesConfigurationProvider configurationProvider;
    private final QueueStatisticsCollector queueStatisticsCollector;
    private final RedisQuesTimer timer;
    private MessageConsumer<String> trimRequestConsumer;
    private Handler<Void> noQueueMoreItemHandler = null;

    // The queues this verticle instance is registered as a consumer
    private final Map<String, QueueProcessingState> myQueues = new HashMap<>();

    public QueueConsumerRunner(Vertx vertx, RedisService redisService, QueueMetrics metrics, QueueStatsService queueStatsService,
                               KeyspaceHelper keyspaceHelper,
                               RedisquesConfigurationProvider configurationProvider, RedisQuesExceptionFactory exceptionFactory,
                               QueueStatisticsCollector queueStatisticsCollector) {
        this.vertx = vertx;
        this.redisService = redisService;
        this.exceptionFactory = exceptionFactory;
        this.metrics = metrics;
        this.queueStatsService = queueStatsService;
        this.keyspaceHelper = keyspaceHelper;
        this.configurationProvider = configurationProvider;
        this.queueStatisticsCollector = queueStatisticsCollector;

        // handles trim request
        trimRequestConsumer = vertx.eventBus().consumer(keyspaceHelper.getTrimRequestKey(), event -> {
            final String queueName = event.body();
            if (queueName == null) {
                log.warn("Got event bus trim request msg with empty body! uid={}  address={}  replyAddress={}", keyspaceHelper.getVerticleUid(), event.address(), event.replyAddress());
                return;
            }
            log.debug("RedisQues got notification for trim queue '{}'", queueName);
            QueueProcessingState queueProcessingState = myQueues.get(queueName);
            if (queueProcessingState == null) {
                log.trace("RedisQues Queue {} is handed by other consumer", queueName);
                return;
            }
            QueueState state = queueProcessingState.getState();
            log.trace("RedisQues consumer: {} queue: {} state: {}", keyspaceHelper.getVerticleUid(), queueName, state);
            if (state != QueueState.CONSUMING) {
                //not in state consuming, trim now
                trimQueueItemIfNeeded(queueName);
            } else {
                log.debug("RedisQues Queue {} is state of consuming, trim will process while consuming", queueName);
            }
        });
        int metricRefreshPeriod = configurationProvider.configuration().getMetricRefreshPeriod();
        if (metricRefreshPeriod > 0) {
            vertx.eventBus().consumer(keyspaceHelper.getMetricsCollectorAddress(), (Handler<Message<Void>>) event -> {
                Map<QueueState, Long> stateCount = getQueueStateCount();
                JsonObject jsonObject = new JsonObject();
                stateCount.forEach((queueState, aLong) -> jsonObject.put(queueState.name(), aLong));
                event.reply(jsonObject);
            });
        }
        timer = new RedisQuesTimer(vertx);
    }

    public Map<String, QueueProcessingState> getMyQueues() {
        return myQueues;
    }

    public void trimRequestConsumerUnregister(Handler<AsyncResult<Void>> handler) {
        trimRequestConsumer.unregister(handler);
    }

    public Future<Void> consume(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        log.debug("RedisQues Requested to consume queue {}", queueName);
        refreshRegistration(queueName).onComplete(event -> {
            if (event.failed()) {
                log.warn("Failed to refresh registration for queue '{}'.", queueName, event.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            metrics.perQueueMetricsRefresh(queueName);
            // Make sure that I am still the registered consumer
            final String consumerKey = keyspaceHelper.getConsumersPrefix() + queueName;
            log.trace("RedisQues consume get: {}", consumerKey);
            redisService.get(consumerKey).onComplete(event1 -> {
                if (event1.failed()) {
                    log.error("Unable to get consumer for queue {}", queueName, event1.cause());
                    return;
                }
                String consumer = Objects.toString(event1.result(), "");
                log.trace("RedisQues refresh registration consumer: {}", consumer);
                if (keyspaceHelper.getVerticleUid().equals(consumer)) {
                    QueueProcessingState queueProcessingState = myQueues.get(queueName);
                    if (queueProcessingState == null) {
                        log.trace("RedisQues Queue {} is already being consumed or handed by other consumer", queueName);
                        promise.complete();
                        return;
                    }
                    QueueState state = queueProcessingState.getState();
                    log.trace("RedisQues consumer: {} queue: {} state: {}", consumer, queueName, state);
                    // Get the next message only once the previous has
                    // been completely processed
                    if (state != QueueState.CONSUMING) {
                        setMyQueuesState(queueName, QueueState.CONSUMING);
                        if (state == null) {
                            // No previous state was stored. Maybe the
                            // consumer was restarted
                            log.warn("Received request to consume from a queue I did not know about: {}", queueName);
                        }
                        // We have item, start to consuming
                        setMyQueuesState(queueName, QueueState.CONSUMING);
                        trimQueueItemIfNeeded(queueName).onComplete(event22 -> {
                            log.trace("RedisQues Starting to consume queue {}", queueName);
                            readQueue(queueName).onComplete(readQueueEvent -> {
                                if (readQueueEvent.failed()) {
                                    log.warn("TODO error handling", exceptionFactory.newException(
                                            "readQueue(" + queueName + ") failed", readQueueEvent.cause()));
                                }
                                promise.complete();
                            });
                        });

                    } else {
                        log.trace("RedisQues Queue {} is already being consumed", queueName);
                        promise.complete();
                    }
                } else {
                    // Somehow registration changed. Let's renotify.
                    log.trace("Registration for queue {} has changed to {}", queueName, consumer);
                    myQueues.remove(queueName);
                    // This queue is not owned by this instance; removing it from the local dequeue statistics cache.
                    queueStatsService.dequeueStatisticRemoveFromLocal(queueName);
                    notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                        if (notifyConsumerEvent.failed()) {
                            log.warn("TODO error handling", exceptionFactory.newException(
                                    "notifyConsumer(" + queueName + ") failed", notifyConsumerEvent.cause()));
                        }
                        promise.complete();
                    });
                }
            });
        });
        return promise.future();
    }

    public void setNoMoreItemHandelr(Handler<Void> handler){

    }

    private Future<Void> readQueue(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        log.trace("RedisQues read queue: {}", queueName);
        String queueKey = keyspaceHelper.getQueuesPrefix() + queueName;
        log.trace("RedisQues read queue lindex: {}", queueKey);

        isQueueLocked(queueName).onComplete(lockAnswer -> {
            if (lockAnswer.failed()) {
                throw exceptionFactory.newRuntimeException("TODO error handling " + queueName, lockAnswer.cause());
            }
            boolean locked = lockAnswer.result();
            if (!locked) {
                redisService.lindex(queueKey, "0").onComplete(answer -> {
                    if (answer.failed()) {
                        log.error("Failed to peek queue '{}'", queueName, answer.cause());
                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    }
                    Response response = answer.result();
                    log.trace("RedisQues read queue lindex result: {}", response);
                    if (response != null) {
                        queueStatsService.createDequeueStatisticIfMissing(queueName);
                        queueStatsService.dequeueStatisticSetLastDequeueAttemptTimestamp(queueName, System.currentTimeMillis());
                        processMessageWithTimeout(queueName, response.toString(), processResult -> {

                            // update the queue failure count and get a retry interval
                            int retryInterval = updateQueueFailureCountAndGetRetryInterval(queueName, processResult.getKey());

                            if (processResult.getKey()) {
                                // Remove the processed message from the queue
                                log.trace("RedisQues read queue lpop: {}", queueKey);
                                redisService.lpop(Collections.singletonList(queueKey)).onComplete(jsonAnswer -> {
                                    if (jsonAnswer.failed()) {
                                        log.error("Failed to pop from queue '{}'", queueName, jsonAnswer.cause());
                                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                                    }

                                    metrics.dequeueCounterIncrement();
                                    log.debug("RedisQues Message removed, queue {} is ready again", queueName);
                                    setMyQueuesState(queueName, QueueState.READY);

                                    Handler<Void> nextMsgHandler = event -> {
                                        // Issue notification to consume next message if any
                                        log.trace("RedisQues read queue: {}", queueKey);
                                        redisService.llen(queueKey).onComplete(answer1 -> {
                                            if (answer1.succeeded() && answer1.result() != null && answer1.result().toInteger() > 0) {
                                                notifyConsumer(queueName).onComplete(event1 -> {
                                                    if (event1.failed())
                                                        log.warn("TODO error handling", exceptionFactory.newException(
                                                                "notifyConsumer(" + queueName + ") failed", event1.cause()));
                                                    promise.complete();
                                                });
                                            } else {
                                                if (answer1.failed() && log.isWarnEnabled()) {
                                                    log.warn("TODO error handling", exceptionFactory.newException(
                                                            "redisAPI.llen(" + queueKey + ") failed", answer1.cause()));
                                                }
                                                promise.complete();
                                            }
                                        });
                                    };

                                    // Notify that we are stopped in case it was the last active consumer
                                    if (noQueueMoreItemHandler != null) {
                                        noQueueMoreItemHandler.handle(null);
                                        return;
                                    }
                                    nextMsgHandler.handle(null);
                                });
                            } else {
                                // Failed. Message will be kept in queue and retried later
                                log.debug("RedisQues Processing failed for queue {}", queueName);
                                // reschedule
                                log.trace("RedisQues will re-send the message to queue '{}' in {} seconds", queueName, retryInterval);
                                rescheduleSendMessageAfterFailure(queueName, retryInterval, processResult.getValue());
                                promise.complete();
                            }
                        });
                    } else {
                        // This can happen when requests to consume happen at the same moment the queue is emptied.
                        log.debug("Got a request to consume from empty queue {}", queueName);
                        setMyQueuesState(queueName, QueueState.READY);
                        queueStatsService.dequeueStatisticMarkedForRemoval(queueName);
                        promise.complete();

                    }
                });
            } else {
                log.debug("Got a request to consume from locked queue {}", queueName);
                setMyQueuesState(queueName, QueueState.READY);
                promise.complete();
            }
        });
        return promise.future();
    }

    private Future<Boolean> isQueueLocked(final String queue) {
        final Promise<Boolean> promise = Promise.promise();
        redisService.hexists(keyspaceHelper.getLocksKey(), queue).onComplete(event -> {
            if (event.failed()) {
                log.warn("Failed to check if queue '{}' is locked. Assume no.", queue, event.cause());
                // TODO:  Is it correct, to assume a queue is not locked in case our query failed?
                // Previous implementation assumed this. See "https://github.com/hiddenalpha/vertx-redisques/blob/v2.5.1/src/main/java/org/swisspush/redisques/RedisQues.java#L856".
                promise.complete(Boolean.FALSE);
            } else if (event.result() == null) {
                promise.complete(Boolean.FALSE);
            } else {
                promise.complete(event.result().toInteger() == 1);
            }
        });
        return promise.future();
    }

    private void rescheduleSendMessageAfterFailure(final String queueName, int retryInSeconds, final String reason) {
        log.trace("RedsQues reschedule after failure for queue: {}", queueName);

        vertx.setTimer(retryInSeconds * 1000L, timerId -> {
            queueStatsService.dequeueStatisticSetNextDequeueDueTimestamp(queueName, System.currentTimeMillis() + retryInSeconds * 1000L, reason);
            if (log.isDebugEnabled()) {
                log.debug("RedisQues re-notify the consumer of queue '{}' at {}", queueName, new Date(System.currentTimeMillis()));
            }
            notifyConsumer(queueName).onComplete(event -> {
                if (event.failed()) {
                    log.warn("TODO error handling", exceptionFactory.newException(
                            "notifyConsumer(" + queueName + ") failed", event.cause()));
                }
                // reset the queue state to be consumed by {@link RedisQues#consume(String)}
                setMyQueuesState(queueName, QueueState.READY);
            });
        });
    }

    private void processMessageWithTimeout(final String queue, final String payload, final Handler<Map.Entry<Boolean, String>> handler) {
        long processorDelayMax = configurationProvider.configuration().getProcessorDelayMax();
        if (processorDelayMax > 0) {
            log.info("About to process message for queue {} with a maximum delay of {}ms", queue, processorDelayMax);
        }
        timer.executeDelayedMax(processorDelayMax).onComplete(delayed -> {
            if (delayed.failed()) {
                log.error("Delayed execution has failed.", exceptionFactory.newException(delayed.cause()));
                handler.handle(new AbstractMap.SimpleEntry<>(false, "Delayed execution has failed."));
                return;
            }
            String processorAddress = configurationProvider.configuration().getProcessorAddress();
            final EventBus eb = vertx.eventBus();
            JsonObject message = new JsonObject();
            message.put("queue", queue);
            message.put(PAYLOAD, payload);
            log.trace("RedisQues process message: {} for queue: {} send it to processor: {}", message, queue, processorAddress);

            // send the message to the consumer
            DeliveryOptions options = new DeliveryOptions().setSendTimeout(configurationProvider.configuration().getProcessorTimeout());
            eb.request(processorAddress, message, options, (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                boolean success;
                String requestMsg = "OK"; // default ok
                if (reply.succeeded()) {
                    JsonObject body = reply.result().body();
                    String status = body.getString(STATUS);
                    success = OK.equals(status);
                    if (success) {
                        queueStatsService.dequeueStatisticSetLastDequeueSuccessTimestamp(queue,System.currentTimeMillis());
                    } else {
                        String msg = body.getString(MESSAGE);
                        StringBuilder sb = new StringBuilder(64 + status.length() + (msg == null ? 0 : msg.length()));
                        sb.append("Queue processor failed with status: ");
                        sb.append(status);
                        sb.append(", Message: ");
                        sb.append(msg);
                        requestMsg = sb.toString();
                    }
                } else {
                    log.info("RedisQues QUEUE_ERROR: Consumer failed {} queue: {}",
                            keyspaceHelper.getVerticleUid(), queue, exceptionFactory.newException(reply.cause()));
                    success = Boolean.FALSE;
                    requestMsg = "Consumer " + keyspaceHelper.getVerticleUid() + " failed with reason: " + reply.cause().getMessage();
                }

                handler.handle(new AbstractMap.SimpleEntry<>(success, requestMsg));
            });
            updateLastQueueProcessTimeStamp(queue);
        });
    }

    private Future<Void> refreshRegistration(String queueName) {
        String address = keyspaceHelper.getVerticleRefreshRegistrationKey();
        return vertx.eventBus().request(address, queueName).transform(ev -> {
            if (ev.failed()) {
                log.error("RedisQues refresh registration failed", ev.cause());
            }
            return ev.succeeded() ? succeededFuture() : failedFuture(ev.cause());
        });
    }

    private Future<Void> notifyConsumer(String queueName) {
        Promise<Void> promise = Promise.promise();
        vertx.eventBus().request(keyspaceHelper.getVerticleNotifyConsumerKey(), queueName).onComplete(event -> {
            if (event.succeeded()) {
                promise.complete();
                return;
            }
            promise.fail(event.cause());
            log.error("RedisQues notify consumer failed", event.cause());
        });
        return promise.future();
    }

    /**
     * trim the queue items, if limit set. this function will always return as a succeededFuture.
     *
     * @param queueName
     * @return Future
     */
    private Future<Void> trimQueueItemIfNeeded(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        final String queueKey = keyspaceHelper.getQueuesPrefix() + queueName;
        final QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
        // trim items if limits set
        if (queueConfiguration != null && queueConfiguration.getMaxQueueEntries() > 0) {
            final int maxQueueEntries = queueConfiguration.getMaxQueueEntries();
            log.debug("RedisQues Max queue entries {} found for queue {}", maxQueueEntries, queueName);
            redisService.ltrim(queueKey, "-" + maxQueueEntries, "-1").onComplete(ltrimResponse -> {
                if (ltrimResponse.failed()) {
                    log.warn("Failed to trim the queue items ", exceptionFactory.newException(
                            "readQueue(" + queueName + ") failed", ltrimResponse.cause()));
                }
                promise.complete();
            });
        } else {
            promise.complete();
        }
        return promise.future();
    }

    /**
     * Update queue consuming state in myQueues
     *
     * @param queueName
     * @param state
     */
    void setMyQueuesState(String queueName, QueueState state) {
        myQueues.compute(queueName, (s, queueProcessingState) -> {
            if (null == queueProcessingState) {
                // not in our list yet
                return new QueueProcessingState(state, 0);
            } else if (queueProcessingState.getState() == QueueState.CONSUMING && state == QueueState.READY) {
                // update the state and the timestamp when we change from CONSUMING to READY
                return new QueueProcessingState(QueueState.READY, currentTimeMillis());
            } else {
                // update the state but leave the timestamp unchanged
                queueProcessingState.setState(state);
                return queueProcessingState;
            }
        });
    }

    /**
     * find first matching Queue-Configuration
     *
     * @param queueName search first configuration for that queue-name
     * @return null when no queueConfiguration's RegEx matches given queueName - else the QueueConfiguration
     */
    private QueueConfiguration findQueueConfiguration(String queueName) {
        for (QueueConfiguration queueConfiguration : configurationProvider.configuration().getQueueConfigurations()) {
            if (queueConfiguration.compiledPattern().matcher(queueName).matches()) {
                return queueConfiguration;
            }
        }
        return null;
    }

    int updateQueueFailureCountAndGetRetryInterval(final String queueName, boolean sendSuccess) {
        if (sendSuccess) {
            queueStatisticsCollector.queueMessageSuccess(queueName, (ex, v) -> {
                if (ex != null) log.warn("TODO error handling", ex);
            });
            return 0;
        } else {
            // update the failure count
            long failureCount = queueStatisticsCollector.queueMessageFailed(queueName);
            // find a retry interval from the queue configurations
            QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
            if (queueConfiguration != null) {
                int[] retryIntervals = queueConfiguration.getRetryIntervals();
                if (retryIntervals != null && retryIntervals.length > 0) {
                    int retryIntervalIndex = (int) (failureCount <= retryIntervals.length ? failureCount - 1 : retryIntervals.length - 1);
                    int retryTime = retryIntervals[retryIntervalIndex];
                    queueStatisticsCollector.setQueueSlowDownTime(queueName, retryTime);
                    return retryTime;
                }
            }
        }
        return configurationProvider.configuration().getRefreshPeriod();
    }

    public Map<QueueState, Long> getQueueStateCount() {
        return myQueues.values().stream()
                .map(QueueProcessingState::getState)
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        () -> new EnumMap<>(QueueState.class),
                        Collectors.counting()
                ));
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queueName name of the queue
     */
    private void updateLastQueueProcessTimeStamp(final String queueName) {
        long ts = System.currentTimeMillis();
        log.trace("RedisQues update timestamp for queue: {} to: {}", queueName, ts);
        redisService.zadd(keyspaceHelper.getQueuesKey(), queueName, String.valueOf(ts)).onFailure(throwable -> {
            log.warn("Redis: Error in updateTimestamp", throwable);
        });
    }

    /**
     * Update the last queue register refreshed time
     * @param queueName
     */
    public void updateLastRefreshRegistrationTimeStamp(String queueName) {
        getMyQueues().computeIfPresent(queueName, (s, queueProcessingState) -> {
            queueProcessingState.setLastRegisterRefreshedMillis(System.currentTimeMillis());
            return queueProcessingState;
        });
    }
}
