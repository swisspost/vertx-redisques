package org.swisspush.redisques;

import com.google.common.base.Strings;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.action.QueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.RedisquesHttpRequestHandler;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newThriftyExceptionFactory;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.OPERATION;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.addQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.bulkDeleteLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.bulkDeleteQueues;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.bulkPutLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteAllLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteAllQueueItems;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteLock;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.enqueue;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getAllLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getConfiguration;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getLock;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueueItems;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueueItemsCount;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueues;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesCount;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesItemsCount;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesSpeed;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesStatistics;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.lockedEnqueue;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.putLock;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.replaceQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.setConfiguration;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

public class RedisQues extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(RedisQues.class);

    // Identifies the consumer
    private final String CONSUMER_UID = UUID.randomUUID().toString();
    private final RedisQuesExceptionFactory exceptionFactory;

    private final Semaphore redisMonitoringReqQuota;
    private final Semaphore checkQueueRequestsQuota;
    private final Semaphore queueStatsRequestQuota;
    private final Semaphore getQueuesItemsCountRedisRequestQuota;

    private MessageConsumer<String> uidMessageConsumer;
    private MessageConsumer<String> consumersMessageConsumer;

    private DequeueStatisticCollector dequeueStatisticCollector;
    private QueueStatisticsCollector queueStatisticsCollector;
    private Map<String, DequeueStatistic> dequeueStatistic = new ConcurrentHashMap<>();
    private boolean dequeueStatisticEnabled = false;

    private Handler<Void> stoppedHandler = null;

    private RedisProvider redisProvider;

    // varia more specific prefixes
    private String queuesKey;
    private String queuesPrefix;
    private String locksKey;

    private RedisQuesTimer timer;
    private MemoryUsageProvider memoryUsageProvider;
    private QueueActionFactory queueActionFactory;
    private RedisquesConfigurationProvider configurationProvider;
    private RedisMonitor redisMonitor;

    private Map<QueueOperation, QueueAction> queueActions = new HashMap<>();

    private PeriodicSkipScheduler periodicSkipScheduler;

    private ActiveQueueService activeQueueService;
    private QueueWatcherService queueWatcherService;
    private QueueRegistrationService queueRegistrationService;

    // State of each queue. Consuming means there is a message being processed.
    enum QueueState {
        READY, CONSUMING
    }

    public RedisQues() {
        this(null, null, null, newThriftyExceptionFactory(), new Semaphore(Integer.MAX_VALUE),
                new Semaphore(Integer.MAX_VALUE), new Semaphore(Integer.MAX_VALUE), new Semaphore(Integer.MAX_VALUE));
        log.warn("Fallback to legacy behavior and allow up to {} simultaneous requests to redis", Integer.MAX_VALUE);
    }

    public RedisQues(
            MemoryUsageProvider memoryUsageProvider,
            RedisquesConfigurationProvider configurationProvider,
            RedisProvider redisProvider,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisMonitoringReqQuota,
            Semaphore checkQueueRequestsQuota,
            Semaphore queueStatsRequestQuota,
            Semaphore getQueuesItemsCountRedisRequestQuota
    ) {
        this.memoryUsageProvider = memoryUsageProvider;
        this.configurationProvider = configurationProvider;
        this.redisProvider = redisProvider;
        this.exceptionFactory = exceptionFactory;
        this.redisMonitoringReqQuota = redisMonitoringReqQuota;
        this.checkQueueRequestsQuota = checkQueueRequestsQuota;
        this.queueStatsRequestQuota = queueStatsRequestQuota;
        this.getQueuesItemsCountRedisRequestQuota = getQueuesItemsCountRedisRequestQuota;
    }

    public static RedisQuesBuilder builder() {
        return new RedisQuesBuilder();
    }

    @Override
    public void stop() {
        queueRegistrationService.unregisterConsumers(activeQueueService.getQueuesState(), true);
        if (redisMonitor != null) {
            redisMonitor.stop();
            redisMonitor = null;
        }
    }

    @Override
    public void start(Promise<Void> promise) {
        log.info("Started with UID {}", CONSUMER_UID);

        if (this.configurationProvider == null) {
            this.configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, config());
        }

        if (this.periodicSkipScheduler == null) {
            this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
        }

        RedisquesConfiguration modConfig = configurationProvider.configuration();
        log.info("Starting Redisques module with configuration: {}", configurationProvider.configuration());

        int dequeueStatisticReportIntervalSec = modConfig.getDequeueStatisticReportIntervalSec();
        if (modConfig.isDequeueStatsEnabled()) {
            dequeueStatisticEnabled = true;
            Runnable publisher = newDequeueStatisticPublisher();
            vertx.setPeriodic(1000L * dequeueStatisticReportIntervalSec, time -> publisher.run());
        }
        if (this.dequeueStatisticCollector == null) {
            this.dequeueStatisticCollector = new DequeueStatisticCollector(vertx, dequeueStatisticEnabled);
        }
        queuesKey = modConfig.getRedisPrefix() + "queues";
        locksKey = modConfig.getRedisPrefix() + "locks";
        queuesPrefix = modConfig.getRedisPrefix() + "queues:";

        timer = new RedisQuesTimer(vertx);

        if (redisProvider == null) {
            redisProvider = new DefaultRedisProvider(vertx, configurationProvider);
        }

        //   this.upperBoundParallel = new UpperBoundParallel(vertx, exceptionFactory);
        this.queueRegistrationService = new QueueRegistrationService(redisProvider, modConfig, CONSUMER_UID, exceptionFactory);

        redisProvider.redis().onComplete(event -> {
            if (event.succeeded()) {
                initialize();
                promise.complete();
            } else {
                promise.fail(new Exception(event.cause()));
            }
        });
    }

    class Task {
        private final String queueName;
        private final DequeueStatistic dequeueStatistic;

        Task(String queueName, DequeueStatistic dequeueStatistic) {
            this.queueName = queueName;
            this.dequeueStatistic = dequeueStatistic;
        }

        Future<Void> execute() {
            // switch to a worker thread
            return vertx.executeBlocking(promise -> {
                dequeueStatisticCollector.setDequeueStatistic(queueName, dequeueStatistic).onComplete(event -> {
                    if (event.failed()) {
                        log.error("Future that should always succeed has failed, ignore it", event.cause());
                    }
                    promise.complete();
                });
            });
        }
    }

    int updateQueueFailureCountAndGetRetryInterval(final String queueName, boolean sendSuccess) {
        if (sendSuccess) {
            queueStatisticsCollector.queueMessageSuccess(queueName, (ex, v) -> {
                if (ex != null) log.warn("TODO_3q98hq3 error handling", ex);
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

    private void initialize() {
        RedisquesConfiguration configuration = configurationProvider.configuration();
        final String queueCheckLastexecKey = configuration.getRedisPrefix() + "check:lastexec";

        this.queueStatisticsCollector = new QueueStatisticsCollector(
                redisProvider, queuesPrefix, vertx, exceptionFactory, redisMonitoringReqQuota,
                configuration.getQueueSpeedIntervalSec());

        RedisquesHttpRequestHandler.init(
                vertx, configuration, queueStatisticsCollector, dequeueStatisticCollector,
                exceptionFactory, queueStatsRequestQuota);

        // only initialize memoryUsageProvider when not provided in the constructor
        if (memoryUsageProvider == null) {
            memoryUsageProvider = new DefaultMemoryUsageProvider(redisProvider, vertx,
                    configurationProvider.configuration().getMemoryUsageCheckIntervalSec());
        }

        assert getQueuesItemsCountRedisRequestQuota != null;
        queueActionFactory = new QueueActionFactory(
                redisProvider, vertx, log, queuesKey, queuesPrefix, queueRegistrationService.getConsumersPrefix(), locksKey,
                memoryUsageProvider, queueStatisticsCollector, exceptionFactory,
                configurationProvider, getQueuesItemsCountRedisRequestQuota);

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
        uidMessageConsumer = vertx.eventBus().consumer(CONSUMER_UID, event -> {
            final String queue = event.body();
            if (queue == null) {
                log.warn("Got event bus msg with empty body! uid={}  address={}  replyAddress={}", CONSUMER_UID, event.address(), event.replyAddress());
                // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
            }
            log.debug("RedisQues got notification for queue '{}'", queue);
            consume(queue);
        });

        this.queueWatcherService = new QueueWatcherService(vertx, configurationProvider, queueRegistrationService,
                exceptionFactory, redisProvider, queuesKey, queueCheckLastexecKey,
                queuesPrefix, queueStatisticsCollector, dequeueStatistic, dequeueStatisticEnabled, checkQueueRequestsQuota);

        this.activeQueueService = new ActiveQueueService(vertx, configurationProvider, queueRegistrationService, queuesKey, CONSUMER_UID,
                exceptionFactory, redisProvider, queueStatisticsCollector, redisMonitoringReqQuota);

        activeQueueService.registerActiveQueueRegistrationRefresh();
        queueWatcherService.registerQueueCheck();

        registerMetricsGathering(configuration);
        registerNotExpiredQueueCheck();
    }

    private void registerNotExpiredQueueCheck() {
        vertx.setPeriodic(20 * 1000, event -> {
            if (!log.isDebugEnabled()) {
                return;
            }
            String keysPattern = queueRegistrationService.getConsumerKey("*");
            log.debug("RedisQues list not expired consumers keys:");
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.scan(Arrays.asList("0", "MATCH", keysPattern, "COUNT", "1000"), keysResult -> {
                        if (keysResult.failed() || keysResult.result() == null || keysResult.result().size() != 2) {
                            log.error("Unable to get redis keys of consumers", keysResult.cause());
                            return;
                        }
                        Response keys = keysResult.result().get(1);
                        if (keys == null || keys.size() == 0) {
                            log.debug("0 not expired consumers keys found");
                            return;
                        }

                        if (log.isTraceEnabled()) {
                            for (Response response : keys) {
                                log.trace(response.toString());
                            }
                        }
                        log.debug("{} not expired consumers keys found, {} keys in myQueues list", keys.size(), activeQueueService.getQueuesState().size());
                    }))
                    .onFailure(throwable -> log.error("Redis: Unable to get redis keys of consumers", throwable));
        });
    }

    private void registerMetricsGathering(RedisquesConfiguration configuration) {
        String metricsAddress = configuration.getPublishMetricsAddress();
        if (Strings.isNullOrEmpty(metricsAddress)) {
            return;
        }
        String metricStorageName = configuration.getMetricStorageName();
        int metricRefreshPeriod = configuration.getMetricRefreshPeriod();

        redisMonitor = new RedisMonitor(vertx, redisProvider, metricsAddress, metricStorageName, metricRefreshPeriod);
        redisMonitor.start();
    }

    private Runnable newDequeueStatisticPublisher() {
        return new Runnable() {
            final AtomicBoolean isRunning = new AtomicBoolean();
            Iterator<Map.Entry<String, DequeueStatistic>> iter;
            long startEpochMs;
            AtomicInteger i = new AtomicInteger();
            int size;

            public void run() {
                if (!isRunning.compareAndSet(false, true)) {
                    log.warn("Previous publish run still in progress at idx {} of {} since {}ms",
                            i, size, currentTimeMillis() - startEpochMs);
                    return;
                }
                try {
                    // Local copy to prevent changes between 'size' and 'iterator' call, plus
                    // to prevent changes of the backing set while we're iterating.
                    Map<String, DequeueStatistic> localCopy = new HashMap<>(dequeueStatistic);
                    size = localCopy.size();
                    iter = localCopy.entrySet().iterator();

                    // use local copy to clean up
                    localCopy.forEach((queueName, dequeueStatistic) -> {
                        if (dequeueStatistic.isMarkedForRemoval()) {
                            RedisQues.this.dequeueStatistic.remove(queueName);
                        }
                    });

                    i.set(0);
                    startEpochMs = currentTimeMillis();
                    if (size > 5_000) log.warn("Going to report {} dequeue statistics towards collector", size);
                    else if (size > 500) log.info("Going to report {} dequeue statistics towards collector", size);
                    else log.trace("Going to report {} dequeue statistics towards collector", size);
                } catch (Throwable ex) {
                    isRunning.set(false);
                    throw ex;
                }
                resume();
            }

            void resume() {
                // here we are executing in an event loop thread
                try {
                    List<Task> entryList = new ArrayList<>();
                    while (iter.hasNext()) {
                        var entry = iter.next();
                        var queueName = entry.getKey();
                        var dequeueStatistic = entry.getValue();
                        entryList.add(new Task(queueName, dequeueStatistic));
                    }

                    Future<List<Void>> startFuture = Future.succeededFuture(new ArrayList<>());
                    // chain the futures sequentially to execute tasks
                    Future<List<Void>> resultFuture = entryList.stream()
                            .reduce(startFuture, (future, task) -> future.compose(previousResults -> {
                                // perform asynchronous task
                                return task.execute().compose(taskResult -> {
                                    // append task result to previous results
                                    previousResults.add(taskResult);
                                    i.incrementAndGet();
                                    return Future.succeededFuture(previousResults);
                                });
                            }), (a, b) -> Future.succeededFuture());
                    resultFuture.onComplete(event -> {
                        if (event.failed()) {
                            log.error("publishing dequeue statistics not complete, just continue", event.cause());
                        }
                        if (log.isTraceEnabled()) {
                            log.trace("Done publishing {} dequeue statistics. Took {}ms", i, currentTimeMillis() - startEpochMs);
                        }
                        isRunning.set(false);
                    });
                } catch (Throwable ex) {
                    isRunning.set(false);
                    throw ex;
                }
            }
        };
    }

    private Handler<Message<JsonObject>> operationsHandler() {
        return event -> {
            final JsonObject body = event.body();
            if (body == null)
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
                    queueWatcherService.checkQueues().onFailure(ex -> {
                        if (log.isWarnEnabled()) {
                            log.warn("TODO error handling", exceptionFactory.newException(ex));
                        }
                    });
                    return;
                case reset:
                    queueRegistrationService.resetConsumers();
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

    private void unsupportedOperation(String operation, Message<JsonObject> event) {
        JsonObject reply = new JsonObject();
        String message = "QUEUE_ERROR: Unsupported operation received: " + operation;
        log.error(message);
        reply.put(STATUS, ERROR);
        reply.put(MESSAGE, message);
        event.reply(reply);
    }

    private void gracefulStop(final Handler<Void> doneHandler) {
        consumersMessageConsumer.unregister(event -> uidMessageConsumer.unregister(unregisterEvent -> {
            if (event.failed()) log.warn("TODO error handling", exceptionFactory.newException(
                    "unregister(" + event + ") failed", event.cause()));
            queueRegistrationService.unregisterConsumers(activeQueueService.getQueuesState(), false).onComplete(unregisterConsumersEvent -> {
                if (unregisterEvent.failed()) {
                    log.warn("TODO error handling", exceptionFactory.newException(
                            "unregisterConsumers() failed", unregisterEvent.cause()));
                }
                stoppedHandler = doneHandler;
                if (activeQueueService.getQueuesState().keySet().isEmpty()) {
                    doneHandler.handle(null);
                }
            });
        }));
    }

    private Future<Void> consume(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        log.debug("RedisQues Requested to consume queue {}", queueName);
        queueRegistrationService.refreshConsumerRegistration(queueName, event -> {
            if (event.failed()) {
                log.warn("Failed to refresh registration for queue '{}'.", queueName, event.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            // Make sure that I am still the registered consumer
            String consumerKey = queueRegistrationService.getConsumerKey(queueName);
            log.trace("RedisQues consume get: {}", consumerKey);
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(consumerKey, event1 -> {
                        if (event1.failed()) {
                            log.error("Unable to get consumer for queue {}", queueName, event1.cause());
                            return;
                        }
                        String consumer = Objects.toString(event1.result(), "");
                        log.trace("RedisQues refresh registration consumer: {}", consumer);
                        if (CONSUMER_UID.equals(consumer)) {
                            QueueState state = activeQueueService.getQueueState(queueName);
                            log.trace("RedisQues consumer: {} queue: {} state: {}", consumer, queueName, state);
                            // Get the next message only once the previous has
                            // been completely processed
                            if (state != QueueState.CONSUMING) {
                                activeQueueService.putQueueState(queueName, QueueState.CONSUMING);
                                if (state == null) {
                                    // No previous state was stored. Maybe the
                                    // consumer was restarted
                                    log.warn("Received request to consume from a queue I did not know about: {}", queueName);
                                }
                                log.trace("RedisQues Starting to consume queue {}", queueName);
                                readQueue(queueName).onComplete(readQueueEvent -> {
                                    if (readQueueEvent.failed()) {
                                        log.warn("TODO error handling", exceptionFactory.newException(
                                                "readQueue(" + queueName + ") failed", readQueueEvent.cause()));
                                    }
                                    promise.complete();
                                });
                            } else {
                                log.trace("RedisQues Queue {} is already being consumed", queueName);
                                promise.complete();
                            }
                        } else {
                            // Somehow registration changed. Let's renotify.
                            log.trace("Registration for queue {} has changed to {}", queueName, consumer);
                            activeQueueService.removeQueueState(queueName);
                            queueWatcherService.notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                                if (notifyConsumerEvent.failed()) {
                                    log.warn("TODO error handling", exceptionFactory.newException(
                                            "notifyConsumer(" + queueName + ") failed", notifyConsumerEvent.cause()));
                                }
                                promise.complete();
                            });
                        }
                    }))
                    .onFailure(throwable -> log.error("Redis: Unable to get consumer for queue {}", queueName, throwable));
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
            if (lockAnswer.failed()) {
                throw exceptionFactory.newRuntimeException("TODO error handling " + queueName, lockAnswer.cause());
            }
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
                        if (dequeueStatisticEnabled) {
                            dequeueStatistic.computeIfAbsent(queueName, s -> {
                                DequeueStatistic newDequeueStatistic = new DequeueStatistic();
                                newDequeueStatistic.setLastDequeueAttemptTimestamp(currentTimeMillis());
                                return new DequeueStatistic();
                            });
                        }
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
                                    activeQueueService.putQueueState(queueName, QueueState.READY);

                                    Handler<Void> nextMsgHandler = event -> {
                                        // Issue notification to consume next message if any
                                        log.trace("RedisQues read queue: {}", queueKey);
                                        redisAPI.llen(queueKey, answer1 -> {
                                            if (answer1.succeeded() && answer1.result() != null && answer1.result().toInteger() > 0) {
                                                queueWatcherService.notifyConsumer(queueName).onComplete(event1 -> {
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
                                    if (stoppedHandler != null) {
                                        queueRegistrationService.unregisterConsumers(activeQueueService.getQueuesState(), false).onComplete(event -> {
                                            if (event.failed()) {
                                                log.warn("TODO error handling", exceptionFactory.newException(
                                                        "unregisterConsumers() failed", event.cause()));
                                            }
                                            if (activeQueueService.getQueuesState().isEmpty()) {
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
                                log.trace("RedisQues will re-send the message to queue '{}' in {} seconds", queueName, retryInterval);
                                rescheduleSendMessageAfterFailure(queueName, retryInterval);
                                promise.complete();
                            }
                        });
                    } else {
                        // This can happen when requests to consume happen at the same moment the queue is emptied.
                        log.debug("Got a request to consume from empty queue {}", queueName);
                        activeQueueService.putQueueState(queueName, QueueState.READY);

                        if (dequeueStatisticEnabled) {
                            dequeueStatistic.computeIfPresent(queueName, (s, dequeueStatistic) -> {
                                dequeueStatistic.setMarkedForRemoval();
                                return dequeueStatistic;
                            });
                        }
                        promise.complete();
                    }
                })).onFailure(throwable -> {
                    // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    log.warn("Redis: Error on readQueue", throwable);
                    activeQueueService.putQueueState(queueName, QueueState.READY);
                    promise.complete();
                });
            } else {
                log.debug("Got a request to consume from locked queue {}", queueName);
                activeQueueService.putQueueState(queueName, QueueState.READY);
                promise.complete();
            }
        });
        return promise.future();
    }

    private void rescheduleSendMessageAfterFailure(final String queueName, int retryInSeconds) {
        log.trace("RedsQues reschedule after failure for queue: {}", queueName);

        vertx.setTimer(retryInSeconds * 1000L, timerId -> {
            if (dequeueStatisticEnabled) {
                dequeueStatistic.computeIfPresent(queueName, (s, dequeueStatistic) -> {
                    long retryDelayInMills = retryInSeconds * 1000L;
                    dequeueStatistic.setNextDequeueDueTimestamp(System.currentTimeMillis() + retryDelayInMills);
                    return dequeueStatistic;
                });
            }
            if (log.isDebugEnabled()) {
                log.debug("RedisQues re-notify the consumer of queue '{}' at {}", queueName, new Date(System.currentTimeMillis()));
            }
            queueWatcherService.notifyConsumer(queueName).onComplete(event -> {
                if (event.failed()) {
                    log.warn("TODO error handling", exceptionFactory.newException(
                            "notifyConsumer(" + queueName + ") failed", event.cause()));
                }
                // reset the queue state to be consumed by {@link RedisQues#consume(String)}
                activeQueueService.putQueueState(queueName, QueueState.READY);
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
                log.error("Delayed execution has failed.", exceptionFactory.newException(delayed.cause()));
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
                    if (success && dequeueStatisticEnabled) {
                        dequeueStatistic.computeIfPresent(queue, (s, dequeueStatistic) -> {
                            dequeueStatistic.setLastDequeueSuccessTimestamp(System.currentTimeMillis());
                            dequeueStatistic.setNextDequeueDueTimestamp(null);
                            return dequeueStatistic;
                        });
                    }
                } else {
                    log.info("RedisQues QUEUE_ERROR: Consumer failed {} queue: {}",
                            CONSUMER_UID, queue, exceptionFactory.newException(reply.cause()));
                    success = Boolean.FALSE;
                }
                handler.handle(success);
            });
            queueRegistrationService.updateTimestamp(queuesKey, queue, null);
        });
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
        log.debug("RedisQues Got registration request for queue {} from consumer: {}", queueName, CONSUMER_UID);
        // Try to register for this queue
        queueRegistrationService.RegisterWithConsumer(queueName, CONSUMER_UID, true, event -> {
            if (event.succeeded()) {
                String value = event.result() != null ? event.result().toString() : null;
                log.trace("RedisQues setxn result: {} for queue: {}", value, queueName);
                if ("OK".equals(value)) {
                    // I am now the registered consumer for this queue.
                    log.debug("RedisQues Now registered for queue {}", queueName);
                    activeQueueService.putQueueState(queueName, QueueState.READY);
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
}
