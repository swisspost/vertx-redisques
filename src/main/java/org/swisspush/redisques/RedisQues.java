package org.swisspush.redisques;

import com.google.common.base.Strings;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.action.QueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.RedisquesHttpRequestHandler;
import org.swisspush.redisques.performance.BurstSquasher;
import org.swisspush.redisques.performance.UpperBoundParallel;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

    public static class RedisQuesBuilder {

        private MemoryUsageProvider memoryUsageProvider;
        private RedisquesConfigurationProvider configurationProvider;
        private RedisProvider redisProvider;
        private RedisQuesExceptionFactory exceptionFactory;
        private Semaphore redisMonitoringReqQuota;
        private Semaphore checkQueueRequestsQuota;
        private Semaphore queueStatsRequestQuota;
        private Semaphore getQueuesItemsCountRedisRequestQuota;

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

        public RedisQuesBuilder withExceptionFactory(RedisQuesExceptionFactory exceptionFactory) {
            this.exceptionFactory = exceptionFactory;
            return this;
        }

        /**
         * How many redis requests monitoring related component will trigger
         * simultaneously. One of those components for example is
         * {@link QueueStatisticsCollector}.
         */
        public RedisQuesBuilder withRedisMonitoringReqQuota(Semaphore quota) {
            this.redisMonitoringReqQuota = quota;
            return this;
        }

        /**
         * How many redis requests {@link RedisQues#checkQueues()} will trigger
         * simultaneously.
         */
        public RedisQuesBuilder withCheckQueueRequestsQuota(Semaphore quota) {
            this.checkQueueRequestsQuota = quota;
            return this;
        }

        /**
         * How many incoming requests {@link QueueStatsService} will accept
         * simultaneously.
         */
        public RedisQuesBuilder withQueueStatsRequestQuota(Semaphore quota) {
            this.queueStatsRequestQuota = quota;
            return this;
        }

        /**
         * How many simultaneous redis requests will be performed maximally for
         * {@link org.swisspush.redisques.handler.GetQueuesItemsCountHandler} requests.
         */
        public RedisQuesBuilder withGetQueuesItemsCountRedisRequestQuota(Semaphore quota) {
            this.getQueuesItemsCountRedisRequestQuota = quota;
            return this;
        }

        public RedisQues build() {
            if (exceptionFactory == null) {
                exceptionFactory = newThriftyExceptionFactory();
            }
            if (redisMonitoringReqQuota == null) {
                redisMonitoringReqQuota = new Semaphore(Integer.MAX_VALUE);
                log.warn("No redis request limit provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
            }
            if (checkQueueRequestsQuota == null) {
                checkQueueRequestsQuota = new Semaphore(Integer.MAX_VALUE);
                log.warn("No redis check queue limit provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
            }
            if (queueStatsRequestQuota == null) {
                queueStatsRequestQuota = new Semaphore(Integer.MAX_VALUE);
                log.warn("No redis queue stats limit provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
            }
            if (getQueuesItemsCountRedisRequestQuota == null) {
                getQueuesItemsCountRedisRequestQuota = new Semaphore(Integer.MAX_VALUE);
                log.warn("No redis getQueueItemsCount quota provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
            }
            return new RedisQues(memoryUsageProvider, configurationProvider, redisProvider, exceptionFactory,
                    redisMonitoringReqQuota, checkQueueRequestsQuota, queueStatsRequestQuota,
                    getQueuesItemsCountRedisRequestQuota);
        }
    }

    // State of each queue. Consuming means there is a message being processed.
    private enum QueueState {
        READY, CONSUMING
    }

    // Identifies the consumer
    private final String uid = UUID.randomUUID().toString();

    private MessageConsumer<String> uidMessageConsumer;
    private UpperBoundParallel upperBoundParallel;

    // The queues this verticle is listening to
    private final Map<String, QueueState> myQueues = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(RedisQues.class);

    private DequeueStatisticCollector dequeueStatisticCollector;
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
    private RedisMonitor redisMonitor;

    private Map<QueueOperation, QueueAction> queueActions = new HashMap<>();

    private Map<String, DequeueStatistic> dequeueStatistic = new ConcurrentHashMap<>();
    private boolean dequeueStatisticEnabled = false;
    private final RedisQuesExceptionFactory exceptionFactory;
    private PeriodicSkipScheduler periodicSkipScheduler;
    private final Semaphore redisMonitoringReqQuota;
    private final Semaphore checkQueueRequestsQuota;
    private final Semaphore queueStatsRequestQuota;
    private final Semaphore getQueuesItemsCountRedisRequestQuota;
    private static final AtomicReference<BurstSquasher<ErrorContext>> checkExistsFailLogger = new AtomicReference<>();

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

        if (this.dequeueStatisticCollector == null) {
            this.dequeueStatisticCollector = new DequeueStatisticCollector(vertx);
        }

        if (this.periodicSkipScheduler == null) {
            this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
        }

        RedisquesConfiguration modConfig = configurationProvider.configuration();
        log.info("Starting Redisques module with configuration: {}", configurationProvider.configuration());

        int dequeueStatisticReportIntervalSec = modConfig.getDequeueStatisticReportIntervalSec();
        if (dequeueStatisticReportIntervalSec > 0) {
            dequeueStatisticEnabled = true;
            Runnable publisher = newDequeueStatisticPublisher();
            vertx.setPeriodic(1000L * dequeueStatisticReportIntervalSec, time -> publisher.run());
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

        this.upperBoundParallel = new UpperBoundParallel(vertx, exceptionFactory);

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
                redisProvider, vertx, log, queuesKey, queuesPrefix, consumersPrefix, locksKey,
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
        registerMetricsGathering(configuration);
    }

    private void registerMetricsGathering(RedisquesConfiguration configuration){
        String metricsAddress = configuration.getPublishMetricsAddress();
        if(Strings.isNullOrEmpty(metricsAddress)) {
            return;
        }
        String metricStorageName = configuration.getMetricStorageName();
        int metricRefreshPeriod = configuration.getMetricRefreshPeriod();

        redisMonitor = new RedisMonitor(vertx, redisProvider, metricsAddress, metricStorageName, metricRefreshPeriod);
        redisMonitor.start();
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
                    else log.debug("Going to report {} dequeue statistics towards collector", size);
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
                            }),  (a,b) -> Future.succeededFuture());
                    resultFuture.onComplete(event -> {
                        if (event.failed()) {
                            log.error("publishing dequeue statistics not complete, just continue", event.cause());
                        }
                        log.debug("Done publishing {} dequeue statistics. Took {}ms", i, currentTimeMillis() - startEpochMs);
                        isRunning.set(false);
                    });
                } catch (Throwable ex) {
                    isRunning.set(false);
                    throw ex;
                }
            }
        };
    }

    private void registerActiveQueueRegistrationRefresh() {
        // Periodic refresh of my registrations on active queues.
        var periodMs = configurationProvider.configuration().getRefreshPeriod() * 1000L;
        periodicSkipScheduler.setPeriodic(periodMs, "registerActiveQueueRegistrationRefresh", new Consumer<Runnable>() {
            Iterator<Map.Entry<String, QueueState>> iter;
            @Override public void accept(Runnable onPeriodicDone) {
                // Need a copy to prevent concurrent modification issuses.
                iter = new HashMap<>(myQueues).entrySet().iterator();
                // Trigger only a limitted amount of requests in parallel.
                upperBoundParallel.request(redisMonitoringReqQuota, iter, new UpperBoundParallel.Mentor<>() {
                    @Override public boolean runOneMore(BiConsumer<Throwable, Void> onQueueDone, Iterator<Map.Entry<String, QueueState>> iter) {
                        handleNextQueueOfInterest(onQueueDone);
                        return iter.hasNext();
                    }
                    @Override public boolean onError(Throwable ex, Iterator<Map.Entry<String, QueueState>> iter) {
                        if (log.isWarnEnabled()) log.warn("TODO error handling", exceptionFactory.newException(ex));
                        onPeriodicDone.run();
                        return false;
                    }
                    @Override public void onDone(Iterator<Map.Entry<String, QueueState>> iter) {
                        onPeriodicDone.run();
                    }
                });
            }
            void handleNextQueueOfInterest(BiConsumer<Throwable, Void> onQueueDone) {
                while (iter.hasNext()) {
                    var entry = iter.next();
                    if (entry.getValue() != QueueState.CONSUMING) continue;
                    checkIfImStillTheRegisteredConsumer(entry.getKey(), onQueueDone);
                    return;
                }
                // no entry found. we're done.
                onQueueDone.accept(null, null);
            }
            void checkIfImStillTheRegisteredConsumer(String queue, BiConsumer<Throwable, Void> onDone) {
                // Check if I am still the registered consumer
                String consumerKey = consumersPrefix + queue;
                log.trace("RedisQues refresh queues get: {}", consumerKey);
                redisProvider.redis().onComplete( ev1 -> {
                    if (ev1.failed()) {
                        onDone.accept(exceptionFactory.newException("redisProvider.redis() failed", ev1.cause()), null);
                        return;
                    }
                    var redisAPI = ev1.result();
                    redisAPI.get(consumerKey, getConsumerEvent -> {
                        if (getConsumerEvent.failed()) {
                            Throwable ex = exceptionFactory.newException(
                                    "Failed to get queue consumer for queue '" + queue + "'", getConsumerEvent.cause());
                            assert ex != null;
                            onDone.accept(ex, null);
                            return;
                        }
                        final String consumer = Objects.toString(getConsumerEvent.result(), "");
                        if (uid.equals(consumer)) {
                            log.debug("RedisQues Periodic consumer refresh for active queue {}", queue);
                            refreshRegistration(queue, ev -> {
                                if (ev.failed()) {
                                    onDone.accept(exceptionFactory.newException("TODO error handling", ev.cause()), null);
                                    return;
                                }
                                updateTimestamp(queue, ev3 -> {
                                    Throwable ex = ev3.succeeded() ? null : exceptionFactory.newException(
                                        "updateTimestamp(" + queue + ") failed", ev3.cause());
                                    onDone.accept(ex, null);
                                });
                            });
                        } else {
                            log.debug("RedisQues Removing queue {} from the list", queue);
                            myQueues.remove(queue);
                            queueStatisticsCollector.resetQueueFailureStatistics(queue, onDone);
                        }
                    });
                });
            }
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
                    checkQueues().onFailure(ex -> {
                        if (log.isWarnEnabled()) {
                            log.warn("TODO error handling", exceptionFactory.newException(ex));
                        }
                    });
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


    private void registerQueueCheck() {
        vertx.setPeriodic(configurationProvider.configuration().getCheckIntervalTimerMs(), periodicEvent -> {
            redisProvider.redis().<Response>compose((RedisAPI redisAPI) -> {
                int checkInterval = configurationProvider.configuration().getCheckInterval();
                return redisAPI.send(Command.SET, queueCheckLastexecKey, String.valueOf(currentTimeMillis()), "NX", "EX", String.valueOf(checkInterval));
            }).<Void>compose((Response todoExplainWhyThisIsIgnored) -> {
                log.info("periodic queue check is triggered now");
                return checkQueues();
            }).onFailure((Throwable ex) -> {
                if (log.isErrorEnabled()) log.error("TODO error handling", exceptionFactory.newException(ex));
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
        if(redisMonitor != null) {
            redisMonitor.stop();
            redisMonitor = null;
        }
    }

    private void gracefulStop(final Handler<Void> doneHandler) {
        consumersMessageConsumer.unregister(event -> uidMessageConsumer.unregister(unregisterEvent -> {
            if (event.failed()) log.warn("TODO error handling", exceptionFactory.newException(
                "unregister(" + event + ") failed", event.cause()));
            unregisterConsumers(false).onComplete(unregisterConsumersEvent -> {
                if( unregisterEvent.failed() ) {
                    log.warn("TODO error handling", exceptionFactory.newException(
                            "unregisterConsumers() failed", unregisterEvent.cause()));
                }
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
                    if (event.failed()) {
                        log.warn("TODO error handling", exceptionFactory.newException(
                            "refreshRegistration(" + queue + ") failed", event.cause()));
                    }
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
            if (ev.failed()) log.warn("TODO error handling", exceptionFactory.newException(ev.cause()));
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
                                    if (readQueueEvent.failed()) {
                                        log.warn("TODO error handling", exceptionFactory.newException(
                                                "readQueue(" + queueName + ") failed", readQueueEvent.cause()));
                                    }
                                    promise.complete();
                                });
                            } else {
                                log.debug("RedisQues Queue {} is already being consumed", queueName);
                                promise.complete();
                            }
                        } else {
                            // Somehow registration changed. Let's renotify.
                            log.debug("Registration for queue {} has changed to {}", queueName, consumer);
                            myQueues.remove(queueName);
                            notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                                if (notifyConsumerEvent.failed()) {
                                    log.warn("TODO error handling", exceptionFactory.newException(
                                        "notifyConsumer(" + queueName + ") failed", notifyConsumerEvent.cause()));
                                }
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
                            dequeueStatistic.computeIfAbsent(queueName, s -> new DequeueStatistic());
                            dequeueStatistic.get(queueName).setLastDequeueAttemptTimestamp(System.currentTimeMillis());
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
                                    myQueues.put(queueName, QueueState.READY);

                                    Handler<Void> nextMsgHandler = event -> {
                                        // Issue notification to consume next message if any
                                        log.trace("RedisQues read queue: {}", queueKey);
                                        redisAPI.llen(queueKey, answer1 -> {
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
                                    if (stoppedHandler != null) {
                                        unregisterConsumers(false).onComplete(event -> {
                                            if (event.failed()) {
                                                log.warn("TODO error handling", exceptionFactory.newException(
                                                    "unregisterConsumers() failed", event.cause()));
                                            }
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
            notifyConsumer(queueName).onComplete(event -> {
                if (event.failed()) {
                    log.warn("TODO error handling", exceptionFactory.newException(
                            "notifyConsumer(" + queueName + ") failed", event.cause()));
                }
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
                            uid, queue, exceptionFactory.newException(reply.cause()));
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
        final var ctx = new Object() {
            long limit;
            RedisAPI redisAPI;
            AtomicInteger counter;
            Iterator<Response> iter;
        };
        return Future.<Void>succeededFuture().<RedisAPI>compose((Void v) -> {
            log.debug("Checking queues timestamps");
            // List all queues that look inactive (i.e. that have not been updated since 3 periods).
            ctx.limit = currentTimeMillis() - 3L * configurationProvider.configuration().getRefreshPeriod() * 1000;
            return redisProvider.redis();
        }).<Response>compose((RedisAPI redisAPI) -> {
            ctx.redisAPI = redisAPI;
            var p = Promise.<Response>promise();
            redisAPI.zrangebyscore(Arrays.asList(queuesKey, "-inf", String.valueOf(ctx.limit)), p);
            return p.future();
        }).<Void>compose((Response queues) -> {
            assert ctx.counter == null;
            assert ctx.iter == null;
            ctx.counter = new AtomicInteger(queues.size());
            ctx.iter = queues.iterator();
            log.trace("RedisQues update queues: {}", ctx.counter);
            var p = Promise.<Void>promise();
            upperBoundParallel.request(checkQueueRequestsQuota, null, new UpperBoundParallel.Mentor<Void>() {
                @Override public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void ctx_) {
                    if (ctx.iter.hasNext()) {
                        var queueObject = ctx.iter.next();
                        // Check if the inactive queue is not empty (i.e. the key exists)
                        final String queueName = queueObject.toString();
                        String key = queuesPrefix + queueName;
                        log.trace("RedisQues update queue: {}", key);
                        Handler<Void> refreshRegHandler = event -> {
                            // Make sure its TTL is correctly set (replaces the previous orphan detection mechanism).
                            refreshRegistration(queueName, refreshRegistrationEvent -> {
                                if (refreshRegistrationEvent.failed()) log.warn("TODO error handling",
                                        exceptionFactory.newException("refreshRegistration(" + queueName + ") failed",
                                        refreshRegistrationEvent.cause()));
                                // And trigger its consumer.
                                notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                                    if (notifyConsumerEvent.failed()) log.warn("TODO error handling",
                                            exceptionFactory.newException("notifyConsumer(" + queueName + ") failed",
                                            notifyConsumerEvent.cause()));
                                    onDone.accept(null, null);
                                });
                            });
                        };
                        ctx.redisAPI.exists(Collections.singletonList(key), event -> {
                            if (event.failed() || event.result() == null) {
                                checkExistsFailLogger.compareAndSet(null, new BurstSquasher<>(vertx, (int count, ErrorContext ctx) -> {
                                    log.error("RedisQues was {} times unable to check existence of some queue like {}",
                                        count, ctx.queueName, ctx.ex);
                                }));
                                checkExistsFailLogger.get().logSomewhen(new ErrorContext(queueName, key,
                                        exceptionFactory.newException("redisAPI.exists(...) failed", event.cause())));
                                onDone.accept(null, null);
                                return;
                            }
                            if (event.result().toLong() == 1) {
                                log.debug("Updating queue timestamp for queue '{}'", queueName);
                                // If not empty, update the queue timestamp to keep it in the sorted set.
                                updateTimestamp(queueName, upTsResult -> {
                                    if (upTsResult.failed()) {
                                        log.warn("Failed to update timestamps for queue '{}'", queueName,
                                            exceptionFactory.newException("updateTimestamp(" + queueName + ") failed",
                                            upTsResult.cause()));
                                        return;
                                    }
                                    // Ensure we clean the old queues after having updated all timestamps
                                    if (ctx.counter.decrementAndGet() == 0) {
                                        removeOldQueues(ctx.limit).onComplete(removeOldQueuesEvent -> {
                                            if (removeOldQueuesEvent.failed() && log.isWarnEnabled()) {
                                                log.warn("TODO error handling", exceptionFactory.newException(
                                                        "removeOldQueues(" + ctx.limit + ") failed", removeOldQueuesEvent.cause()));
                                            }
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
                                if (dequeueStatisticEnabled) {
                                    dequeueStatistic.computeIfPresent(queueName, (s, dequeueStatistic) -> {
                                        dequeueStatistic.setMarkedForRemoval();
                                        return dequeueStatistic;
                                    });
                                }
                                if (ctx.counter.decrementAndGet() == 0) {
                                    removeOldQueues(ctx.limit).onComplete(removeOldQueuesEvent -> {
                                        if (removeOldQueuesEvent.failed() && log.isWarnEnabled()) {
                                            log.warn("TODO error handling", exceptionFactory.newException(
                                                "removeOldQueues(" + ctx.limit + ") failed", removeOldQueuesEvent.cause()));
                                        }
                                        queueStatisticsCollector.resetQueueFailureStatistics(queueName, onDone);
                                    });
                                } else {
                                    queueStatisticsCollector.resetQueueFailureStatistics(queueName, onDone);
                                }
                            }
                        });
                    }
                    return ctx.iter.hasNext();
                }
                @Override public boolean onError(Throwable ex, Void ctx_) {
                    for (Throwable waitQueFullEx = ex; waitQueFullEx != null; waitQueFullEx = waitQueFullEx.getCause()) {
                        if (!(waitQueFullEx instanceof NoStackTraceThrowable)) continue;
                        if (!"Redis waiting queue is full".equals(waitQueFullEx.getMessage())) continue;
                        // Trying to continue (aka return true) makes no sense as our redis
                        // client is out of resources and continuing just will make load
                        // situation worse. So we abort in the case of this special exception.
                        makeGcLifeEasier();
                        p.fail(exceptionFactory.newException(ex));
                        return false;
                    }
                    log.warn("TODO error handling", exceptionFactory.newException(ex));
                    // Still not sure if it is a good idea to ignore errors and try to respond
                    // with half-baked results. But will keep it this way because old code [1]
                    // did without any comment that would have explained why.
                    // [1]: https://github.com/swisspost/vertx-redisques/blob/v3.0.33/src/main/java/org/swisspush/redisques/RedisQues.java
                    return true; // true, aka keep going with other queues.
                }
                @Override public void onDone(Void ctx_) {
                    makeGcLifeEasier();
                    p.complete(); // Mark this composition step as completed.
                }
                private void makeGcLifeEasier() {
                    // No longer used, so reduce GC graph traversal effort.
                    ctx.redisAPI = null;
                    ctx.counter = null;
                    ctx.iter = null;
                }
            });
            return p.future();
        });
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
                        if (event.failed() && log.isWarnEnabled()) log.warn("TODO error handling",
                                exceptionFactory.newException("redisAPI.zremrangebyscore('" + queuesKey + "', '-inf', " + limit + ") failed",
                                event.cause()));
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

    private static class ErrorContext {
        String queueName; String key; Throwable ex;
        public ErrorContext(String queueName, String key, Throwable ex) {
            this.queueName = queueName; this.key = key; this.ex = ex;
        }
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
