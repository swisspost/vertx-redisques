package org.swisspush.redisques;

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.action.QueueAction;
import org.swisspush.redisques.handler.RedisquesHttpRequestHandler;
import org.swisspush.redisques.util.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.swisspush.redisques.util.RedisquesAPI.*;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.*;

public class RedisQues extends AbstractVerticle {

    public static class RedisQuesBuilder {

        private MemoryUsageProvider memoryUsageProvider;
        private RedisquesConfigurationProvider configurationProvider;
        private RedisProvider redisProvider;

        private RedisQuesBuilder() {
            // Private, as clients should use "RedisQues.builder()" and not this class here directly.
        }

        public RedisQuesBuilder withMemoryUsageProvider(MemoryUsageProvider memoryUsageProvider) {
            this.memoryUsageProvider = memoryUsageProvider;
            return this;
        }

        public RedisQuesBuilder withRedisquesRedisquesConfigurationProvider(RedisquesConfigurationProvider configurationProvider) {
            this.configurationProvider = configurationProvider;
            return this;
        }

        public RedisQuesBuilder withRedisProvider(RedisProvider redisProvider) {
            this.redisProvider = redisProvider;
            return this;
        }

        public RedisQues build() {
            return new RedisQues(memoryUsageProvider, configurationProvider, redisProvider);
        }
    }

    // State of each queue. Consuming means there is a message being processed.
    private enum QueueState {
        READY, CONSUMING
    }

    // Identifies the consumer
    private final String uid = UUID.randomUUID().toString();

    private MessageConsumer<String> uidMessageConsumer;

    // The queues this verticle is listening to
    private final Map<String, QueueState> myQueues = new HashMap<>();

    private final Logger log = LoggerFactory.getLogger(RedisQues.class);

    private QueueStatisticsCollector queueStatisticsCollector;

    private Handler<Void> stoppedHandler = null;

    private MessageConsumer<String> consumersMessageConsumer;

    // Configuration

    private RedisProvider redisProvider;

    // varia more specific prefixes
    private String queuesKey;
    private String queuesPrefix;
    private String consumersPrefix;
    private String locksKey;
    private String queueCheckLastexecKey;

    private int consumerLockTime;

    private RedisQuesTimer timer;
    private MemoryUsageProvider memoryUsageProvider;
    private QueueActionFactory queueActionFactory;
    private RedisquesConfigurationProvider configurationProvider;

    private Map<QueueOperation, QueueAction> queueActions = new HashMap<>();

    private Map<String, DequeueStatistic> dequeueStatistic = new ConcurrentHashMap<>();

    public RedisQues() {
    }

    public RedisQues(MemoryUsageProvider memoryUsageProvider, RedisquesConfigurationProvider configurationProvider,
                     RedisProvider redisProvider) {
        this.memoryUsageProvider = memoryUsageProvider;
        this.configurationProvider = configurationProvider;
        this.redisProvider = redisProvider;
    }

    public static RedisQuesBuilder builder() {
        return new RedisQuesBuilder();
    }

    private void redisSetWithOptions(String key, String value, boolean nx, int ex,
                                     Handler<AsyncResult<Response>> handler) {
        JsonArray options = new JsonArray();
        options.add("EX").add(ex);
        if (nx) {
            options.add("NX");
        }
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.send(
                        Command.SET, RedisUtils.toPayload(key, value, options).toArray(new String[0]))
                .onComplete(handler)).onFailure(throwable -> handler.handle(new FailedAsyncResult<>(throwable)));
    }

    /**
     * <p>Handler receiving registration requests when no consumer is registered
     * for a queue.</p>
     */
    private void handleRegistrationRequest(Message<String> msg) {
        final String queueName = msg.body();
        if (queueName == null) {
            log.warn("Got message without queue name while handleRegistrationRequest.");
            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
        }
        log.debug("RedisQues Got registration request for queue {} from consumer: {}", queueName, uid);
        // Try to register for this queue
        redisSetWithOptions(consumersPrefix + queueName, uid, true, consumerLockTime, event -> {
            if (event.succeeded()) {
                String value = event.result() != null ? event.result().toString() : null;
                log.trace("RedisQues setxn result: {} for queue: {}", value, queueName);
                if ("OK".equals(value)) {
                    // I am now the registered consumer for this queue.
                    log.debug("RedisQues Now registered for queue {}", queueName);
                    myQueues.put(queueName, QueueState.READY);
                    consume(queueName);
                } else {
                    log.debug("RedisQues Missed registration for queue {}", queueName);
                    // Someone else just became the registered consumer. I
                    // give up.
                }
            } else {
                log.error("redisSetWithOptions failed", event.cause());
            }
        });
    }

    @Override
    public void start(Promise<Void> promise) {
        log.info("Started with UID {}", uid);

        if (this.configurationProvider == null) {
            this.configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, config());
        }
        RedisquesConfiguration modConfig = configurationProvider.configuration();
        log.info("Starting Redisques module with configuration: {}", configurationProvider.configuration());

        int dequeueStatisticReportInterval = modConfig.getDequeueStatisticReportIntervalSec();
        if (dequeueStatisticReportInterval > 0) {
            vertx.setPeriodic(1000L * dequeueStatisticReportInterval, handler -> {
                dequeueStatistic.forEach((queueName, dequeueStatistic) ->
                        queueStatisticsCollector.setDequeueStatistic(queueName, dequeueStatistic));
            });
        }

        queuesKey = modConfig.getRedisPrefix() + "queues";
        queuesPrefix = modConfig.getRedisPrefix() + "queues:";
        consumersPrefix = modConfig.getRedisPrefix() + "consumers:";
        locksKey = modConfig.getRedisPrefix() + "locks";
        queueCheckLastexecKey = modConfig.getRedisPrefix() + "check:lastexec";
        consumerLockTime = 2 * modConfig.getRefreshPeriod(); // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        timer = new RedisQuesTimer(vertx);

        if (redisProvider == null) {
            redisProvider = new DefaultRedisProvider(vertx, configurationProvider);
        }

        redisProvider.redis().onComplete(event -> {
            if(event.succeeded()) {
                initialize();
                promise.complete();
            } else {
                promise.fail(new Exception(event.cause()));
            }
        });
    }

    private void initialize() {
        RedisquesConfiguration configuration = configurationProvider.configuration();
        this.queueStatisticsCollector = new QueueStatisticsCollector(redisProvider,
                queuesPrefix, vertx, configuration.getQueueSpeedIntervalSec());

        RedisquesHttpRequestHandler.init(vertx, configuration, queueStatisticsCollector);

        // only initialize memoryUsageProvider when not provided in the constructor
        if (memoryUsageProvider == null) {
            memoryUsageProvider = new DefaultMemoryUsageProvider(redisProvider, vertx,
                    configurationProvider.configuration().getMemoryUsageCheckIntervalSec());
        }

        queueActionFactory = new QueueActionFactory(redisProvider, vertx, log,
                queuesKey, queuesPrefix, consumersPrefix, locksKey, queueStatisticsCollector, memoryUsageProvider,
                configurationProvider);

        queueActions.put(addQueueItem, queueActionFactory.buildQueueAction(addQueueItem));
        queueActions.put(deleteQueueItem, queueActionFactory.buildQueueAction(deleteQueueItem));
        queueActions.put(deleteAllQueueItems, queueActionFactory.buildQueueAction(deleteAllQueueItems));
        queueActions.put(bulkDeleteQueues, queueActionFactory.buildQueueAction(bulkDeleteQueues));
        queueActions.put(replaceQueueItem, queueActionFactory.buildQueueAction(replaceQueueItem));
        queueActions.put(getQueueItem, queueActionFactory.buildQueueAction(getQueueItem));
        queueActions.put(getQueueItems, queueActionFactory.buildQueueAction(getQueueItems));
        queueActions.put(getQueues, queueActionFactory.buildQueueAction(getQueues));
        queueActions.put(getQueuesCount, queueActionFactory.buildQueueAction(getQueuesCount));
        queueActions.put(getQueueItemsCount, queueActionFactory.buildQueueAction(getQueueItemsCount));
        queueActions.put(getQueuesItemsCount, queueActionFactory.buildQueueAction(getQueuesItemsCount));
        queueActions.put(enqueue, queueActionFactory.buildQueueAction(enqueue));
        queueActions.put(lockedEnqueue, queueActionFactory.buildQueueAction(lockedEnqueue));
        queueActions.put(getLock, queueActionFactory.buildQueueAction(getLock));
        queueActions.put(putLock, queueActionFactory.buildQueueAction(putLock));
        queueActions.put(bulkPutLocks, queueActionFactory.buildQueueAction(bulkPutLocks));
        queueActions.put(getAllLocks, queueActionFactory.buildQueueAction(getAllLocks));
        queueActions.put(deleteLock, queueActionFactory.buildQueueAction(deleteLock));
        queueActions.put(bulkDeleteLocks, queueActionFactory.buildQueueAction(bulkDeleteLocks));
        queueActions.put(deleteAllLocks, queueActionFactory.buildQueueAction(deleteAllLocks));
        queueActions.put(getQueuesSpeed, queueActionFactory.buildQueueAction(getQueuesSpeed));
        queueActions.put(getQueuesStatistics, queueActionFactory.buildQueueAction(getQueuesStatistics));
        queueActions.put(setConfiguration, queueActionFactory.buildQueueAction(setConfiguration));
        queueActions.put(getConfiguration, queueActionFactory.buildQueueAction(getConfiguration));

        String address = configuration.getAddress();

        // Handles operations
        vertx.eventBus().consumer(address, operationsHandler());

        // Handles registration requests
        consumersMessageConsumer = vertx.eventBus().consumer(address + "-consumers", this::handleRegistrationRequest);

        // Handles notifications
        uidMessageConsumer = vertx.eventBus().consumer(uid, event -> {
            final String queue = event.body();
            if (queue == null) {
                log.warn("Got event bus msg with empty body! uid={}  address={}  replyAddress={}", uid, event.address(), event.replyAddress());
                // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
            }
            log.debug("RedisQues got notification for queue '{}'", queue);
            consume(queue);
        });

        registerActiveQueueRegistrationRefresh();
        registerQueueCheck();
    }

    private void registerActiveQueueRegistrationRefresh() {
        // Periodic refresh of my registrations on active queues.
        vertx.setPeriodic(configurationProvider.configuration().getRefreshPeriod() * 1000L, event -> {
            // Check if I am still the registered consumer
            myQueues.entrySet().stream().filter(entry -> entry.getValue() == QueueState.CONSUMING).
                    forEach(entry -> {
                        final String queue = entry.getKey();
                        // Check if I am still the registered consumer
                        String consumerKey = consumersPrefix + queue;
                        if (log.isTraceEnabled()) {
                            log.trace("RedisQues refresh queues get: {}", consumerKey);
                        }
                        redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(consumerKey, getConsumerEvent -> {
                                    if (getConsumerEvent.failed()) {
                                        log.warn("Failed to get queue consumer for queue '{}'. But we'll continue anyway :)", queue, getConsumerEvent.cause());
                                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                                    }
                                    final String consumer = Objects.toString(getConsumerEvent.result(), "");
                                    if (uid.equals(consumer)) {
                                        log.debug("RedisQues Periodic consumer refresh for active queue {}", queue);
                                        refreshRegistration(queue, ev -> {
                                            if (ev.failed())
                                                log.warn("TODO error handling", new Exception(ev.cause()));
                                            updateTimestamp(queue, null);
                                        });
                                    } else {
                                        log.debug("RedisQues Removing queue {} from the list", queue);
                                        myQueues.remove(queue);
                                        queueStatisticsCollector.resetQueueFailureStatistics(queue);
                                    }
                                }))
                                .onFailure(throwable -> log.error("Redis: Failed to registerActiveQueueRegistrationRefresh", throwable));
                    });
        });
    }

    private Handler<Message<JsonObject>> operationsHandler() {
        return event -> {
            final JsonObject body = event.body();
            if( body == null )
                throw new NullPointerException("Why is body empty? addr=" + event.address() + "  replyAddr=" + event.replyAddress());
            String operation = body.getString(OPERATION);
            log.trace("RedisQues got operation: {}", operation);

            QueueOperation queueOperation = QueueOperation.fromString(operation);
            if (queueOperation == null) {
                unsupportedOperation(operation, event);
                return;
            }

            // handle system operations
            switch (queueOperation) {
                case check:
                    checkQueues();
                    return;
                case reset:
                    resetConsumers();
                    return;
                case stop:
                    gracefulStop(aVoid -> {/*no-op*/});
                    return;
            }

            // handle queue operations
            QueueAction action = queueActions.getOrDefault(queueOperation, queueActionFactory.buildUnsupportedAction());
            action.execute(event);
        };
    }

    int updateQueueFailureCountAndGetRetryInterval(final String queueName, boolean sendSuccess) {
        if (sendSuccess) {
            queueStatisticsCollector.queueMessageSuccess(queueName);
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

    private void registerQueueCheck() {
        vertx.setPeriodic(configurationProvider.configuration().getCheckIntervalTimerMs(), periodicEvent -> {
            redisProvider.connection()
                    .onFailure(ex -> log.error("TODO error handling", new Exception(ex)))
                    .onSuccess(conn -> {
                        conn.send(Request.cmd(Command.SET, queueCheckLastexecKey, System.currentTimeMillis(),
                                        "NX", "EX", configurationProvider.configuration().getCheckInterval()))
                                .onFailure(ex -> log.error("Unexpected queue check result", new Exception(ex)))
                                .onSuccess(response -> {
                                    log.info("periodic queue check is triggered now");
                                    checkQueues();
                                });
                    });
        });
    }

    private void unsupportedOperation(String operation, Message<JsonObject> event) {
        JsonObject reply = new JsonObject();
        String message = "QUEUE_ERROR: Unsupported operation received: " + operation;
        log.error(message);
        reply.put(STATUS, ERROR);
        reply.put(MESSAGE, message);
        event.reply(reply);
    }

    @Override
    public void stop() {
        unregisterConsumers(true);
    }

    private void gracefulStop(final Handler<Void> doneHandler) {
        consumersMessageConsumer.unregister(event -> uidMessageConsumer.unregister(unregisterEvent -> {
            if( event.failed() ) log.warn("TODO error handling", new Exception(event.cause()));
            unregisterConsumers(false).onComplete(unregisterConsumersEvent -> {
                if( unregisterEvent.failed() )
                    log.warn("TODO error handling", new Exception(unregisterEvent.cause()));
                stoppedHandler = doneHandler;
                if (myQueues.keySet().isEmpty()) {
                    doneHandler.handle(null);
                }
            });
        }));
    }

    private Future<Void> unregisterConsumers(boolean force) {
        final Promise<Void> result = Promise.promise();
        log.debug("RedisQues unregister consumers. force={}", force);
        final List<Future> futureList = new ArrayList<>(myQueues.size());
        for (final Map.Entry<String, QueueState> entry : myQueues.entrySet()) {
            final Promise<Void> promise = Promise.promise();
            futureList.add(promise.future());
            final String queue = entry.getKey();
            if (force || entry.getValue() == QueueState.READY) {
                log.trace("RedisQues unregister consumers queue: {}", queue);
                refreshRegistration(queue, event -> {
                    if( event.failed() ) log.warn("TODO error handling", new Exception(event.cause()));
                    // Make sure that I am still the registered consumer
                    String consumerKey = consumersPrefix + queue;
                    log.trace("RedisQues unregister consumers get: {}", consumerKey);
                    redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(consumerKey, getEvent -> {
                        if (getEvent.failed()) {
                            log.warn("Failed to retrieve consumer '{}'.", consumerKey, getEvent.cause());
                            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                        }
                        String consumer = Objects.toString(getEvent.result(), "");
                        log.trace("RedisQues unregister consumers get result: {}", consumer);
                        if (uid.equals(consumer)) {
                            log.debug("RedisQues remove consumer: {}", uid);
                            myQueues.remove(queue);
                        }
                        promise.complete();
                    })).onFailure(throwable -> {
                        log.warn("Failed to retrieve consumer '{}'.", consumerKey, throwable);
                        promise.complete();
                    });
                });
            } else {
                promise.complete();
            }
        }
        CompositeFuture.all(futureList).onComplete(ev -> {
            if( ev.failed() ) log.warn("TODO error handling", new Exception(ev.cause()));
            result.complete();
        });
        return result.future();
    }

    /**
     * Caution: this may in some corner case violate the ordering for one
     * message.
     */
    private void resetConsumers() {
        log.debug("RedisQues Resetting consumers");
        String keysPattern = consumersPrefix + "*";
        log.trace("RedisQues reset consumers keys: {}", keysPattern);
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.keys(keysPattern, keysResult -> {
                    if (keysResult.failed() || keysResult.result() == null) {
                        log.error("Unable to get redis keys of consumers", keysResult.cause());
                        return;
                    }
                    Response keys = keysResult.result();
                    if (keys == null || keys.size() == 0) {
                        log.debug("No consumers found to reset");
                        return;
                    }
                    List<String> args = new ArrayList<>(keys.size());
                    for (Response response : keys) {
                        args.add(response.toString());
                    }
                    redisAPI.del(args, delManyResult -> {
                        if (delManyResult.succeeded()) {
                            if( log.isDebugEnabled() )
                                log.debug("Successfully reset {} consumers", delManyResult.result().toLong());
                        } else {
                            log.error("Unable to delete redis keys of consumers");
                        }
                    });
                }))
                .onFailure(throwable -> log.error("Redis: Unable to get redis keys of consumers", throwable));
    }

    private Future<Void> consume(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        log.debug("RedisQues Requested to consume queue {}", queueName);
        refreshRegistration(queueName, event -> {
            if (event.failed()) {
                log.warn("Failed to refresh registration for queue '{}'.", queueName, event.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            // Make sure that I am still the registered consumer
            String consumerKey = consumersPrefix + queueName;
            log.trace("RedisQues consume get: {}", consumerKey);
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(consumerKey, event1 -> {
                        if (event1.failed()) {
                            log.error("Unable to get consumer for queue " + queueName, event1.cause());
                            return;
                        }
                        String consumer = Objects.toString(event1.result(), "");
                        log.trace("RedisQues refresh registration consumer: {}", consumer);
                        if (uid.equals(consumer)) {
                            QueueState state = myQueues.get(queueName);
                            log.trace("RedisQues consumer: {} queue: {} state: {}", consumer, queueName, state);
                            // Get the next message only once the previous has
                            // been completely processed
                            if (state != QueueState.CONSUMING) {
                                myQueues.put(queueName, QueueState.CONSUMING);
                                if (state == null) {
                                    // No previous state was stored. Maybe the
                                    // consumer was restarted
                                    log.warn("Received request to consume from a queue I did not know about: {}", queueName);
                                }
                                log.debug("RedisQues Starting to consume queue {}", queueName);
                                readQueue(queueName).onComplete(readQueueEvent -> {
                                    if( readQueueEvent.failed() )
                                        log.warn("TODO error handling", new Exception(readQueueEvent.cause()));
                                    promise.complete();
                                });
                            } else {
                                log.debug("RedisQues Queue {} is already being consumed", queueName);
                                promise.complete();
                            }
                        } else {
                            // Somehow registration changed. Let's renotify.
                            log.warn("Registration for queue {} has changed to {}", queueName, consumer);
                            myQueues.remove(queueName);
                            notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                                if( notifyConsumerEvent.failed() )
                                    log.warn("TODO error handling", notifyConsumerEvent.cause());
                                promise.complete();
                            });
                        }
                    }))
                    .onFailure(throwable -> log.error("Redis: Unable to get consumer for queue " + queueName, throwable));
        });
        return promise.future();
    }

    private Future<Boolean> isQueueLocked(final String queue) {
        final Promise<Boolean> promise = Promise.promise();
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.hexists(locksKey, queue, event -> {
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
        })).onFailure(throwable -> {
            log.warn("Redis: Failed to check if queue '{}' is locked. Assume no.", queue, throwable);
            // TODO:  Is it correct, to assume a queue is not locked in case our query failed?
            // Previous implementation assumed this. See "https://github.com/hiddenalpha/vertx-redisques/blob/v2.5.1/src/main/java/org/swisspush/redisques/RedisQues.java#L856".
            promise.complete(Boolean.FALSE);
        });
        return promise.future();
    }

    private Future<Void> readQueue(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        log.trace("RedisQues read queue: {}", queueName);
        String queueKey = queuesPrefix + queueName;
        log.trace("RedisQues read queue lindex: {}", queueKey);

        isQueueLocked(queueName).onComplete(lockAnswer -> {
            if( lockAnswer.failed() )
                throw new UnsupportedOperationException("TODO error handling " + queueName, lockAnswer.cause());
            boolean locked = lockAnswer.result();
            if (!locked) {
                redisProvider.redis().onSuccess(redisAPI -> redisAPI.lindex(queueKey, "0", answer -> {
                    if (answer.failed()) {
                        log.error("Failed to peek queue '{}'", queueName, answer.cause());
                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    }
                    Response response = answer.result();
                    log.trace("RedisQues read queue lindex result: {}", response);
                    if (response != null) {
                        dequeueStatistic.computeIfAbsent(queueName, s -> new DequeueStatistic());
                        dequeueStatistic.get(queueName).lastDequeueAttemptTimestamp = System.currentTimeMillis();
                        processMessageWithTimeout(queueName, response.toString(), success -> {

                            // update the queue failure count and get a retry interval
                            int retryInterval = updateQueueFailureCountAndGetRetryInterval(queueName, success);

                            if (success) {
                                // Remove the processed message from the queue
                                log.trace("RedisQues read queue lpop: {}", queueKey);
                                redisAPI.lpop(Collections.singletonList(queueKey), jsonAnswer -> {
                                    if (jsonAnswer.failed()) {
                                        log.error("Failed to pop from queue '{}'", queueName, jsonAnswer.cause());
                                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                                    }
                                    log.debug("RedisQues Message removed, queue {} is ready again", queueName);
                                    myQueues.put(queueName, QueueState.READY);

                                    Handler<Void> nextMsgHandler = event -> {
                                        // Issue notification to consume next message if any
                                        log.trace("RedisQues read queue: {}", queueKey);
                                        redisAPI.llen(queueKey, answer1 -> {
                                            if (answer1.succeeded() && answer1.result() != null && answer1.result().toInteger() > 0) {
                                                notifyConsumer(queueName).onComplete(event1 -> {
                                                    if( event1.failed() )
                                                        log.warn("TODO error handling", new Exception(event1.cause()));
                                                    promise.complete();
                                                });
                                            } else {
                                                if( answer1.failed() )
                                                    log.warn("TODO error handling", new Exception(answer1.cause()));
                                                promise.complete();
                                            }
                                        });
                                    };

                                    // Notify that we are stopped in case it was the last active consumer
                                    if (stoppedHandler != null) {
                                        unregisterConsumers(false).onComplete(event -> {
                                            if( event.failed() )
                                                log.warn("TODO error handling", new Exception(event.cause()));
                                            if (myQueues.isEmpty()) {
                                                stoppedHandler.handle(null);
                                            }
                                            nextMsgHandler.handle(null);
                                        });
                                        return;
                                    }
                                    nextMsgHandler.handle(null);
                                });
                            } else {
                                // Failed. Message will be kept in queue and retried later
                                log.debug("RedisQues Processing failed for queue {}", queueName);
                                // reschedule
                                log.debug("RedisQues will re-send the message to queue '{}' in {} seconds", queueName, retryInterval);
                                rescheduleSendMessageAfterFailure(queueName, retryInterval);
                                promise.complete();
                            }
                        });
                    } else {
                        // This can happen when requests to consume happen at the same moment the queue is emptied.
                        log.debug("Got a request to consume from empty queue {}", queueName);
                        myQueues.put(queueName, QueueState.READY);
                        dequeueStatistic.remove(queueName);
                        promise.complete();
                    }
                })).onFailure(throwable -> {
                    // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    log.warn("Redis: Error on readQueue", throwable);
                    myQueues.put(queueName, QueueState.READY);
                    promise.complete();
                });
            } else {
                log.debug("Got a request to consume from locked queue {}", queueName);
                myQueues.put(queueName, QueueState.READY);
                promise.complete();
            }
        });
        return promise.future();
    }

    private void rescheduleSendMessageAfterFailure(final String queueName, int retryInSeconds) {
        log.trace("RedsQues reschedule after failure for queue: {}", queueName);

        vertx.setTimer(retryInSeconds * 1000L, timerId -> {
            long retryDelayInMills = retryInSeconds * 1000L;
            dequeueStatistic.get(queueName).nextDequeueDueTimestamp = System.currentTimeMillis() + retryDelayInMills;
            if (log.isDebugEnabled()) {
                log.debug("RedisQues re-notify the consumer of queue '{}' at {}", queueName, new Date(System.currentTimeMillis()));
            }
            notifyConsumer(queueName).onComplete(event -> {
                if( event.failed() ) log.warn("TODO error handling", new Exception(event.cause()));
                // reset the queue state to be consumed by {@link RedisQues#consume(String)}
                myQueues.put(queueName, QueueState.READY);
            });
        });
    }

    private void processMessageWithTimeout(final String queue, final String payload, final Handler<Boolean> handler) {
        long processorDelayMax = configurationProvider.configuration().getProcessorDelayMax();
        if (processorDelayMax > 0) {
            log.info("About to process message for queue {} with a maximum delay of {}ms", queue, processorDelayMax);
        }
        timer.executeDelayedMax(processorDelayMax).onComplete(delayed -> {
            if (delayed.failed()) {
                log.error("Delayed execution has failed.", new Exception(delayed.cause()));
                // TODO: May we should call handler with failed state now.
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
                if (reply.succeeded()) {
                    success = OK.equals(reply.result().body().getString(STATUS));
                    dequeueStatistic.get(queue).lastDequeueSuccessTimestamp = System.currentTimeMillis();
                    dequeueStatistic.get(queue).nextDequeueDueTimestamp = null;
                } else {
                    log.info("RedisQues QUEUE_ERROR: Consumer failed {} queue: {}",
                            uid, queue, new Exception(reply.cause()));
                    success = Boolean.FALSE;
                }
                handler.handle(success);
            });
            updateTimestamp(queue, null);
        });
    }

    private Future<Void> notifyConsumer(final String queueName) {
        log.debug("RedisQues Notifying consumer of queue {}", queueName);
        final EventBus eb = vertx.eventBus();
        final Promise<Void> promise = Promise.promise();
        // Find the consumer to notify
        String key = consumersPrefix + queueName;
        log.trace("RedisQues notify consumer get: {}", key);
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(key, event -> {
                    if (event.failed()) {
                        log.warn("Failed to get consumer for queue '{}'", queueName, new Exception(event.cause()));
                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    }
                    String consumer = Objects.toString(event.result(), null);
                    log.trace("RedisQues got consumer: {}", consumer);
                    if (consumer == null) {
                        // No consumer for this queue, let's make a peer become consumer
                        log.debug("RedisQues Sending registration request for queue {}", queueName);
                        eb.send(configurationProvider.configuration().getAddress() + "-consumers", queueName);
                        promise.complete();
                    } else {
                        // Notify the registered consumer
                        log.debug("RedisQues Notifying consumer {} to consume queue {}", consumer, queueName);
                        eb.send(consumer, queueName);
                        promise.complete();
                    }
                }))
                .onFailure(throwable -> {
                    log.warn("Redis: Failed to get consumer for queue '{}'", queueName, throwable);
                    // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    promise.complete();
                });
        return promise.future();
    }

    private void refreshRegistration(String queueName, Handler<AsyncResult<Response>> handler) {
        log.debug("RedisQues Refreshing registration of queue {}, expire in {} s", queueName, consumerLockTime);
        String consumerKey = consumersPrefix + queueName;
        if (handler == null) {
            throw new RuntimeException("Handler must be set");
        } else {
            redisProvider.redis().onSuccess(redisAPI ->
                            redisAPI.expire(List.of(consumerKey, String.valueOf(consumerLockTime)), handler))
                    .onFailure(throwable -> {
                        log.warn("Redis: Failed to refresh registration of queue {}", queueName, throwable);
                        handler.handle(new FailedAsyncResult<>(throwable));
                    });
        }
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queueName name of the queue
     * @param handler   (optional) To get informed when done.
     */
    private void updateTimestamp(final String queueName, Handler<AsyncResult<Response>> handler) {
        long ts = System.currentTimeMillis();
        log.trace("RedisQues update timestamp for queue: {} to: {}", queueName, ts);
        redisProvider.redis().onSuccess(redisAPI -> {
            if (handler == null) {
                redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName));
            } else {
                redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName), handler);
            }
        }).onFailure(throwable -> {
            log.warn("Redis: Error in updateTimestamp", throwable);
            if (handler != null) {
                handler.handle(new FailedAsyncResult<>(throwable));
            }
        });
    }

    /**
     * Notify not-active/not-empty queues to be processed (e.g. after a reboot).
     * Check timestamps of not-active/empty queues.
     * This uses a sorted set of queue names scored by last update timestamp.
     */
    private Future<Void> checkQueues() {
        final Promise<Void> result = Promise.promise();
        log.debug("Checking queues timestamps");
        // List all queues that look inactive (i.e. that have not been updated since 3 periods).
        final long limit = System.currentTimeMillis() - 3L * configurationProvider.configuration().getRefreshPeriod() * 1000;
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.zrangebyscore(Arrays.asList(queuesKey, "-inf", String.valueOf(limit)), answer -> {
                    Response queues = answer.result();
                    if (answer.failed() || queues == null) {
                        log.error("RedisQues is unable to get list of queues", answer.cause());
                        result.complete();
                        return;
                    }
                    final AtomicInteger counter = new AtomicInteger(queues.size());
                    log.trace("RedisQues update queues: {}", counter);
                    final List<Future> futureList = new ArrayList<>(queues.size());
                    for (Response queueObject : queues) {
                        final Promise<Void> promise = Promise.promise();
                        futureList.add(promise.future());
                        // Check if the inactive queue is not empty (i.e. the key exists)
                        final String queueName = queueObject.toString();
                        String key = queuesPrefix + queueName;
                        log.trace("RedisQues update queue: {}", key);

                        Handler<Void> refreshRegHandler = event -> {
                            // Make sure its TTL is correctly set (replaces the previous orphan detection mechanism).
                            refreshRegistration(queueName, refreshRegistrationEvent -> {
                                if( refreshRegistrationEvent.failed() )
                                    log.warn("TODO error handling", new Exception(refreshRegistrationEvent.cause()));
                                // And trigger its consumer.
                                notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                                    if( notifyConsumerEvent.failed() )
                                        log.warn("TODO error handling", new Exception(notifyConsumerEvent.cause()));
                                    promise.complete();
                                });
                            });
                        };
                        redisAPI.exists(Collections.singletonList(key), event -> {
                            if (event.failed() || event.result() == null) {
                                log.error("RedisQues is unable to check existence of queue " + queueName, event.cause());
                                promise.complete();
                                return;
                            }
                            if (event.result().toLong() == 1) {
                                log.debug("Updating queue timestamp for queue '{}'", queueName);
                                // If not empty, update the queue timestamp to keep it in the sorted set.
                                updateTimestamp(queueName, upTsResult -> {
                                    if (upTsResult.failed()) {
                                        log.warn("Failed to update timestamps for queue '{}'", queueName, upTsResult.cause());
                                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                                    }
                                    // Ensure we clean the old queues after having updated all timestamps
                                    if (counter.decrementAndGet() == 0) {
                                        removeOldQueues(limit).onComplete(removeOldQueuesEvent -> {
                                            if (removeOldQueuesEvent.failed())
                                                log.warn("TODO error handling", new Exception(removeOldQueuesEvent.cause()));
                                            refreshRegHandler.handle(null);
                                        });
                                    } else {
                                        refreshRegHandler.handle(null);
                                    }
                                });
                            } else {
                                // Ensure we clean the old queues also in the case of empty queue.
                                if (log.isTraceEnabled()) {
                                    log.trace("RedisQues remove old queue: {}", queueName);
                                }
                                dequeueStatistic.remove(queueName);
                                if (counter.decrementAndGet() == 0) {
                                    removeOldQueues(limit).onComplete(removeOldQueuesEvent -> {
                                        if( removeOldQueuesEvent.failed() )
                                            log.warn("TODO error handling", new Exception(removeOldQueuesEvent.cause()));
                                        queueStatisticsCollector.resetQueueFailureStatistics(queueName);
                                        promise.complete();
                                    });
                                } else {
                                    queueStatisticsCollector.resetQueueFailureStatistics(queueName);
                                    promise.complete();
                                }
                            }
                        });
                    }
                    CompositeFuture.all(futureList).onComplete(ev1 -> {
                        if( ev1.failed() ) log.warn("Cannot happen", new Exception(ev1.cause()));
                        result.complete();
                    });
                }))
                .onFailure(throwable -> {
                    log.warn("Redis: Failed to checkQueues", throwable);
                    result.complete();
                });
        return result.future();
    }

    /**
     * Remove queues from the sorted set that are timestamped before a limit time.
     *
     * @param limit limit timestamp
     */
    private Future<Void> removeOldQueues(long limit) {
        final Promise<Void> promise = Promise.promise();
        log.debug("Cleaning old queues");
        redisProvider.redis()
                .onSuccess(redisAPI -> {
                    redisAPI.zremrangebyscore(queuesKey, "-inf", String.valueOf(limit), event -> {
                        if( event.failed() ) log.warn("TODO error handling", event.cause());
                        promise.complete();
                    });
                })
                .onFailure(throwable -> {
                    log.warn("Redis: Failed to removeOldQueues", throwable);
                    promise.complete();
                });
        return promise.future();
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

    private class FailedAsyncResult<Response> implements AsyncResult<Response> {

        private final Throwable cause;

        private FailedAsyncResult(Throwable cause) {
            this.cause = cause;
        }

        @Override
        public Response result() {
            return null;
        }

        @Override
        public Throwable cause() {
            return cause;
        }

        @Override
        public boolean succeeded() {
            return false;
        }

        @Override
        public boolean failed() {
            return true;
        }
    }
}
