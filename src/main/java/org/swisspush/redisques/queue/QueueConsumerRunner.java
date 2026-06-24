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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.QueueStatsService;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisQuesTimer;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.util.RedisquesAPI.BATCH_QUEUE;
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
    private final int consumerLockTime;
    private final QueueConfigurationProvider queueConfigurationProvider;
    private MessageConsumer<String> trimRequestConsumer;
    private Handler<Void> noQueueMoreItemHandler = null;

    // The queues this verticle instance is registered as a consumer
    private final Map<String, QueueProcessingState> myQueues = new HashMap<>();

    public QueueConsumerRunner(Vertx vertx, RedisService redisService, QueueMetrics metrics, QueueStatsService queueStatsService,
                               KeyspaceHelper keyspaceHelper,
                               RedisquesConfigurationProvider configurationProvider, RedisQuesExceptionFactory exceptionFactory,
                               QueueStatisticsCollector queueStatisticsCollector, QueueConfigurationProvider queueConfigurationProvider) {
        this.vertx = vertx;
        this.redisService = redisService;
        this.exceptionFactory = exceptionFactory;
        this.metrics = metrics;
        this.queueStatsService = queueStatsService;
        this.keyspaceHelper = keyspaceHelper;
        this.configurationProvider = configurationProvider;
        this.queueStatisticsCollector = queueStatisticsCollector;
        consumerLockTime = configurationProvider.configuration().getConsumerLockMultiplier() * configurationProvider.configuration().getRefreshPeriod(); // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        this.queueConfigurationProvider = queueConfigurationProvider;

        // handles trim request
        trimRequestConsumer = vertx.eventBus().consumer(keyspaceHelper.getTrimRequestKey() + keyspaceHelper.getVerticleUid(), event -> {
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
        // Make sure that I am still the registered consumer
        refreshRegistrationAndGet(queueName).onComplete(event1 -> {
                if (event1.failed()) {
                    log.error("Unable to get consumer for queue {}", queueName, event1.cause());
                    return;
                }
                metrics.perQueueMetricsRefresh(queueName);
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
        return promise.future();
    }

    public void setNoMoreItemHandelr(Handler<Void> handler){
        noQueueMoreItemHandler = handler;
    }

    private Future<Void> readQueue(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        log.trace("RedisQues read queue: {}", queueName);
        isQueueLocked(queueName).onComplete(lockAnswer -> {
            if (lockAnswer.failed()) {
                throw exceptionFactory.newRuntimeException("failed to check lock for queue: " + queueName, lockAnswer.cause());
            }
            boolean locked = lockAnswer.result();
            if (!locked) {
                QueueConfigurationProvider.BatchQueueItemsConfig batchQueueItemsConfig = queueConfigurationProvider.findBatchQueueItemsConfig(queueName);
                if (batchQueueItemsConfig == null || batchQueueItemsConfig.maximumItemInBatchDispatch <= 1) {
                    processSingleItem(queueName).onComplete(event -> {
                        if (event.failed()) {
                            log.error("RedisQues failed to process single item {}", queueName, event.cause());
                            promise.fail(event.cause());
                        } else {
                            promise.complete();
                        }
                    });
                } else {
                    final int maximumItemInBatchDispatch = batchQueueItemsConfig.maximumItemInBatchDispatch;
                    final int minimumItemInBatchDispatch = batchQueueItemsConfig.minimumItemInBatchDispatch;
                    final int maxBatchItemDispatchWaitTimeout = batchQueueItemsConfig.maxBatchItemDispatchWaitTimeout;

                    log.debug("RedisQues process multiple (Max: {}, Min: {}, Timeout(Sec): {}) items of: {}", maximumItemInBatchDispatch, minimumItemInBatchDispatch, maxBatchItemDispatchWaitTimeout, queueName);
                    processMultipleItems(queueName, maximumItemInBatchDispatch, minimumItemInBatchDispatch, maxBatchItemDispatchWaitTimeout).onComplete(event -> {
                        if (event.failed()) {
                            log.error("RedisQues failed to process multiple item {}", queueName, event.cause());
                            promise.fail(event.cause());
                        } else {
                            promise.complete();
                        }
                    });
                }
            } else {
                log.debug("Got a request to consume from locked queue {}", queueName);
                setMyQueuesState(queueName, QueueState.READY);
                promise.complete();
            }
        });
        return promise.future();
    }

    private Future<Void> processMessage(String queueName, String message, int messageSize) {
        Promise<Void> promise = Promise.promise();
        String queueKey = keyspaceHelper.getQueuesPrefix() + queueName;
        queueStatsService.createDequeueStatisticIfMissing(queueName);
        queueStatsService.dequeueStatisticSetLastDequeueAttemptTimestamp(queueName, System.currentTimeMillis());
        processMessageWithTimeout(queueName, message, messageSize > 1, processResult -> {

            // update the queue failure count and get a retry interval
            int retryInterval = updateQueueFailureCountAndGetRetryInterval(queueName, processResult.getKey());

            if (processResult.getKey()) {
                // Remove the processed message from the queue
                log.trace("RedisQues read queue lmpop: {}", queueKey);
                redisService.lpop(List.of(queueKey, String.valueOf(messageSize))).onComplete(jsonAnswer -> {
                    if (jsonAnswer.failed()) {
                        log.error("Failed to pop from queue '{}'", queueName, jsonAnswer.cause());
                        promise.fail(jsonAnswer.cause());
                        return;
                    }

                    metrics.dequeueCounterIncrement();
                    log.debug("RedisQues Message removed, queue {} is ready again", queueName);
                    setMyQueuesState(queueName, QueueState.READY);

                    Handler<Void> nextMsgHandler = event -> {
                        // Issue notification to consume next message if any
                        log.trace("RedisQues read queue: {}", queueKey);
                        redisService.llen(queueKey).onComplete(answer1 -> {
                            if (answer1.succeeded() && answer1.result() != null) {
                                if ( answer1.result().toLong() > 0L) {
                                    notifyConsumer(queueName).onComplete(event1 -> {
                                        if (event1.failed())
                                            log.warn("TODO error handling", exceptionFactory.newException(
                                                    "notifyConsumer(" + queueName + ") failed", event1.cause()));
                                        promise.complete();
                                    });
                                } else {
                                    // Notify that we are stopped in case it was the last active consumer
                                    if (noQueueMoreItemHandler != null) {
                                        noQueueMoreItemHandler.handle(null);
                                    }
                                    promise.complete();
                                }
                                myQueues.computeIfPresent(queueName, (s, queueProcessingState) -> {
                                    queueProcessingState.setQueueItemSize(answer1.result().toInteger());
                                    return queueProcessingState;
                                });
                            } else {
                                if (answer1.failed() && log.isWarnEnabled()) {
                                    log.warn("Failed to get queue item size of {}", queueName, exceptionFactory.newException(
                                            "redisAPI.llen(" + queueKey + ") failed", answer1.cause()));
                                }
                                promise.complete();
                            }
                        });
                    };
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
        return promise.future();
    }

    /**
     * dispatch mutilple queue items of a queue at once,
     * @param queueName     queue name
     * @param maximumItemInBatchDispatch    max items allowed in a batch, if condition reached, will dispatch immediately
     * @param minimumItemInBatchDispatch    min items needed in a batch, if not enough will wait until have enough item
     * @param maxBatchItemDispatchWaitTimeout wait timeout for the queue item, if timeout occurred will dispatch what we have now, ignore the minimumItemInBatchDispatch
     * @return
     */
    Future<Void> processMultipleItems(final String queueName,
                                      final int maximumItemInBatchDispatch,
                                      final int minimumItemInBatchDispatch,
                                      final int maxBatchItemDispatchWaitTimeout) {
        Promise<Void> promise = Promise.promise();
        final String queueKey = keyspaceHelper.getQueuesPrefix() + queueName;

        Handler<Void> nextMsgsHandler = event -> {
            redisService.lrange(queueKey, "0", String.valueOf(maximumItemInBatchDispatch - 1)).onComplete(answer -> {
                if (answer.failed()) {
                    log.error("Failed to peek items of queue '{}'", queueName, answer.cause());
                    promise.fail(answer.cause());
                    return;
                }
                Response response = answer.result();
                log.trace("RedisQues read queue lrange item size: {}", response.size());
                JsonArray itemsJsonArray = new JsonArray();
                for (Response res : response) {
                    itemsJsonArray.add(res.toString());
                }
                processMessage(queueName, itemsJsonArray.toString(), maximumItemInBatchDispatch).onComplete(processMessageAnswer -> {
                    if (processMessageAnswer.failed()) {
                        log.error("Failed to process queue '{}'", queueName, processMessageAnswer.cause());
                        promise.fail(processMessageAnswer.cause());
                    } else {
                        promise.complete();
                    }
                });
            });
        };

        if (minimumItemInBatchDispatch > 0) {
            // have minimum item limit
            long lastConsumedTimestamp = System.currentTimeMillis();
            if (myQueues.containsKey(queueName)) {
                // we have lastConsumedTimestamp in list
                lastConsumedTimestamp = myQueues.get(queueName).getLastConsumedTimestampMillis() != 0 ?
                        myQueues.get(queueName).getLastConsumedTimestampMillis() : lastConsumedTimestamp;
            }

            final long timeFromLastConsumed = currentTimeMillis() - lastConsumedTimestamp;

            if (maxBatchItemDispatchWaitTimeout > 0 && timeFromLastConsumed >= maxBatchItemDispatchWaitTimeout * 1000L) {
                // timeout, just send what we have with itemsInBatch
                nextMsgsHandler.handle(null);
            } else {
                // check do we have enough items in the queue for minimum batch request
                redisService.llen(queueKey).onComplete(answer1 -> {
                    if (answer1.succeeded() && answer1.result() != null) {
                        final long itemSize = answer1.result().toLong();
                        myQueues.computeIfPresent(queueName, (s, queueProcessingState) -> {
                            queueProcessingState.setQueueItemSize(itemSize);
                            return queueProcessingState;
                        });
                        if (itemSize >= minimumItemInBatchDispatch) {
                            //we reached the minimum limit, dispatch
                            nextMsgsHandler.handle(null);
                        } else {
                            // not time out, not reach the limit, just continue with default process delay
                            long processorDelayMax = configurationProvider.configuration().getProcessorDelayMax();
                            timer.executeDelayedMax(processorDelayMax).onComplete(delayed -> {
                                if (delayed.failed()) {
                                    log.error("Delayed execution has failed.", exceptionFactory.newException(delayed.cause()));
                                }
                                setMyQueuesState(queueName, QueueState.READY);
                                promise.complete();
                            });
                        }
                    } else {
                        if (answer1.failed() && log.isWarnEnabled()) {
                            log.warn("Failed to get queue item size of {}", queueName, exceptionFactory.newException(
                                    "redisAPI.llen(" + queueKey + ") failed", answer1.cause()));
                        }
                        promise.fail(answer1.cause());
                    }
                });
            }
        } else {
            // no minimum limit, just dispatch in batch, also ignore timeout
            nextMsgsHandler.handle(null);
        }

        return promise.future();
    }

    Future<Void> processSingleItem(String queueName) {
        Promise<Void> promise = Promise.promise();
        String queueKey = keyspaceHelper.getQueuesPrefix() + queueName;
        redisService.lindex(queueKey, "0").onComplete(answer -> {
            if (answer.failed()) {
                log.error("Failed to peek queue '{}'", queueName, answer.cause());
                promise.fail(answer.cause());
                return;
            }
            Response response = answer.result();
            log.trace("RedisQues read queue lindex result: {}", response);
            if (response != null) {
                processMessage(queueName, response.toString(), 1).onComplete(processMessageAnswer -> {
                    if (processMessageAnswer.failed()) {
                        log.error("Failed to process queue '{}'", queueName, processMessageAnswer.cause());
                        promise.fail(processMessageAnswer.cause());
                    } else {
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

    private void processMessageWithTimeout(final String queue, final String payload, final boolean isBatch, final Handler<Map.Entry<Boolean, String>> handler) {
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
            if (isBatch) {
                message.put(BATCH_QUEUE, true);
            }
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

    private Future<String> refreshRegistrationAndGet(String queueName) {
        log.debug("RedisQues Refreshing registration of queue consumer {}, expire in {} s", queueName, consumerLockTime);
        Promise<String> promise = Promise.promise();
        final String consumerKey = keyspaceHelper.getConsumersPrefix() + queueName;

        List<Request> batch = new ArrayList<>(2);
        batch.add(Request.cmd(Command.EXPIRE).arg(consumerKey).arg(String.valueOf(consumerLockTime)));
        batch.add(Request.cmd(Command.GET).arg(consumerKey));
        redisService.batch(batch).onComplete(res -> {
            // we have 2 commands in a batch, expected response
            if (res.failed() || res.result().size() != 2){
                log.warn("refreshRegistrationAndGet failed with queue: {}", queueName, res.cause());
                promise.fail(res.cause().getMessage());
            } else {
                List<Response> responses = res.result();
                updateLastRefreshRegistrationTimeStamp(queueName);
                if (responses.get(1) != null) {
                    // get the value from GET command
                    promise.complete(responses.get(1).toString());
                } else {
                    log.warn("refreshRegistrationAndGet failed with queue: {}, queue does not exist in register", queueName);
                    promise.fail("queue does not exist: " + queueName);
                }
            }
        });
        return promise.future();
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
        final Integer maxQueueEntries = queueConfigurationProvider.findMaxQueueEntriesConfig(queueName);
        // trim items if limits set
        if (maxQueueEntries != null && maxQueueEntries > 0) {
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
            List<Integer> retryIntervals = queueConfigurationProvider.findRetryIntervalConfig(queueName);
            if (retryIntervals != null && !retryIntervals.isEmpty()) {
                int retryIntervalIndex = (int) (failureCount <= retryIntervals.size() ? failureCount - 1 : retryIntervals.size() - 1);
                int retryTime = retryIntervals.get(retryIntervalIndex);
                queueStatisticsCollector.setQueueSlowDownTime(queueName, retryTime);
                return retryTime;
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
