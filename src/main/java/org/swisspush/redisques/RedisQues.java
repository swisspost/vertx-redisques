package org.swisspush.redisques;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.RedisquesHttpRequestHandler;
import org.swisspush.redisques.lock.Lock;
import org.swisspush.redisques.lock.impl.RedisBasedLock;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.QueueActionsService;
import org.swisspush.redisques.queue.QueueMetrics;
import org.swisspush.redisques.queue.QueueProcessingState;
import org.swisspush.redisques.queue.QueueRegistryService;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.queue.UnregisterConsumerType;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.*;

import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newThriftyExceptionFactory;
import static org.swisspush.redisques.util.RedisquesAPI.*;

public class RedisQues extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(RedisQues.class);

    public final static String TRIM_REQUEST_KEY = "trim_request_";

    // Identifies the consumer
    public String getUid() {
        return uid;
    }
    private final String uid = UUID.randomUUID().toString();
    private MessageConsumer<String> trimRequestConsumer;

    // The queues this verticle instance is registered as a consumer
    public final Map<String, QueueProcessingState> myQueues = new HashMap<>();

    private DequeueStatisticCollector dequeueStatisticCollector;
    private QueueStatisticsCollector queueStatisticsCollector;

    private Handler<Void> stoppedHandler = null;

    private RedisProvider redisProvider;
    private RedisService redisService;
    private QueueRegistryService queueRegistryService;
    private KeyspaceHelper keyspaceHelper;

    // varia more specific prefixes
    private String queuesKey;
    private String queuesPrefix;
    private String consumersPrefix;
    private String locksKey;
    private String queueCheckLastexecKey;


    private int consumerLockTime;

    private RedisQuesTimer timer;
    private MemoryUsageProvider memoryUsageProvider;
    private RedisquesConfigurationProvider configurationProvider;
    private RedisMonitor redisMonitor;
    private QueueActionsService queueActionsService;
    private QueueStatsService queueStatsService;
    private QueueMetrics queueMetrics;

    private MeterRegistry meterRegistry;

    private Map<String, Long> aliveConsumers = new ConcurrentHashMap<>();
    private final RedisQuesExceptionFactory exceptionFactory;
    private PeriodicSkipScheduler periodicSkipScheduler;
    private final Semaphore redisMonitoringReqQuota;
    private final Semaphore checkQueueRequestsQuota;
    private final Semaphore queueStatsRequestQuota;
    private final Semaphore getQueuesItemsCountRedisRequestQuota;
    private final Semaphore activeQueueRegRefreshReqQuota;
    private Lock lock;

    public RedisQues() {
        this(null, null, null, newThriftyExceptionFactory(), new Semaphore(Integer.MAX_VALUE),
                new Semaphore(Integer.MAX_VALUE), new Semaphore(Integer.MAX_VALUE), new Semaphore(Integer.MAX_VALUE),
                new Semaphore(Integer.MAX_VALUE));
        log.warn("Fallback to legacy behavior and allow up to {} simultaneous requests to redis", Integer.MAX_VALUE);
    }

    public RedisQues(
            MemoryUsageProvider memoryUsageProvider,
            RedisquesConfigurationProvider configurationProvider,
            RedisProvider redisProvider,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisMonitoringReqQuota,
            Semaphore activeQueueRegRefreshReqQuota,
            Semaphore checkQueueRequestsQuota,
            Semaphore queueStatsRequestQuota,
            Semaphore getQueuesItemsCountRedisRequestQuota
    ) {
        this(memoryUsageProvider, configurationProvider, redisProvider, exceptionFactory, redisMonitoringReqQuota,
                activeQueueRegRefreshReqQuota, checkQueueRequestsQuota, queueStatsRequestQuota, getQueuesItemsCountRedisRequestQuota, null);
    }

    public RedisQues(
        MemoryUsageProvider memoryUsageProvider,
        RedisquesConfigurationProvider configurationProvider,
        RedisProvider redisProvider,
        RedisQuesExceptionFactory exceptionFactory,
        Semaphore redisMonitoringReqQuota,
        Semaphore activeQueueRegRefreshReqQuota,
        Semaphore checkQueueRequestsQuota,
        Semaphore queueStatsRequestQuota,
        Semaphore getQueuesItemsCountRedisRequestQuota,
        MeterRegistry meterRegistry
    ) {
        this.memoryUsageProvider = memoryUsageProvider;
        this.configurationProvider = configurationProvider;
        this.redisProvider = redisProvider;
        this.exceptionFactory = exceptionFactory;
        this.redisMonitoringReqQuota = redisMonitoringReqQuota;
        this.activeQueueRegRefreshReqQuota = activeQueueRegRefreshReqQuota;
        this.checkQueueRequestsQuota = checkQueueRequestsQuota;
        this.queueStatsRequestQuota = queueStatsRequestQuota;
        this.getQueuesItemsCountRedisRequestQuota = getQueuesItemsCountRedisRequestQuota;
        this.meterRegistry = meterRegistry;
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
                queueMetrics.perQueueMetricsReg(queueName);
                String value = event.result() != null ? event.result().toString() : null;
                log.trace("RedisQues setxn result: {} for queue: {}", value, queueName);
                if (configurationProvider.configuration().getMicrometerMetricsEnabled()) {
                    queueMetrics.consumerCounterIncrement(1);
                }
                if ("OK".equals(value)) {
                    // I am now the registered consumer for this queue.
                    log.debug("RedisQues Now registered for queue {}", queueName);
                    setMyQueuesState(queueName, QueueState.READY);
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

        if (this.periodicSkipScheduler == null) {
            this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
        }

        RedisquesConfiguration modConfig = configurationProvider.configuration();
        log.info("Starting Redisques module with configuration: {}", configurationProvider.configuration());

        if (this.dequeueStatisticCollector == null) {
            this.dequeueStatisticCollector = new DequeueStatisticCollector(vertx, modConfig.isDequeueStatsEnabled());
        }
        queuesKey = modConfig.getRedisPrefix() + "queues";
        queuesPrefix = modConfig.getRedisPrefix() + "queues:";
        consumersPrefix = modConfig.getRedisPrefix() + "consumers:";
        locksKey = modConfig.getRedisPrefix() + "locks";
        queueCheckLastexecKey = modConfig.getRedisPrefix() + "check:lastexec";
        consumerLockTime = modConfig.getConsumerLockMultiplier() * modConfig.getRefreshPeriod(); // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        // the time we let an empty queue live before we deregister ourselves
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
        redisService = new RedisService(redisProvider);
    }

    private void initialize() {
        RedisquesConfiguration configuration = configurationProvider.configuration();
        this.keyspaceHelper = new KeyspaceHelper(configuration, uid);
        this.queueStatisticsCollector = new QueueStatisticsCollector(
                redisService, keyspaceHelper.getQueuesPrefix(), vertx, exceptionFactory, redisMonitoringReqQuota,
                configuration.getQueueSpeedIntervalSec());
        this.lock = new RedisBasedLock(redisService, exceptionFactory);

        this.queueStatsService = new QueueStatsService(
                vertx, configuration, keyspaceHelper.getAddress(), queueStatisticsCollector, dequeueStatisticCollector,
                exceptionFactory, queueStatsRequestQuota);

        this.queueMetrics = new QueueMetrics(vertx, keyspaceHelper, redisService, meterRegistry, configurationProvider, exceptionFactory);
        queueMetrics.initMicrometerMetrics();

        int metricRefreshPeriod = configurationProvider.configuration().getMetricRefreshPeriod();
        if (metricRefreshPeriod > 0) {
            vertx.eventBus().consumer(keyspaceHelper.getMetricsCollectorAddress(), (Handler<Message<Void>>) event -> {
                Map<QueueState, Long> stateCount = getQueueStateCount();
                JsonObject jsonObject = new JsonObject();
                stateCount.forEach((queueState, aLong) -> jsonObject.put(queueState.name(), aLong));
                event.reply(jsonObject);
            });
        }
        RedisquesHttpRequestHandler.init(vertx, configuration, queueStatsService, exceptionFactory);

        // only initialize memoryUsageProvider when not provided in the constructor
        if (memoryUsageProvider == null) {
            memoryUsageProvider = new DefaultMemoryUsageProvider(redisService, vertx,
                    configurationProvider.configuration().getMemoryUsageCheckIntervalSec());
        }
        assert getQueuesItemsCountRedisRequestQuota != null;
        queueActionsService = new QueueActionsService(vertx, redisService, keyspaceHelper, configurationProvider,
                exceptionFactory, memoryUsageProvider, queueStatisticsCollector, getQueuesItemsCountRedisRequestQuota, meterRegistry);
        queueRegistryService = new QueueRegistryService(vertx, redisService, configurationProvider, exceptionFactory, keyspaceHelper, queueMetrics, queueStatsService, queueStatisticsCollector , checkQueueRequestsQuota, activeQueueRegRefreshReqQuota, this);

        String address = configuration.getAddress();

        // Handles operations
        vertx.eventBus().consumer(address, operationsHandler());

        // handles trim request
        trimRequestConsumer = vertx.eventBus().consumer(TRIM_REQUEST_KEY + uid, event -> {
            final String queueName = event.body();
            if (queueName == null) {
                log.warn("Got event bus trim request msg with empty body! uid={}  address={}  replyAddress={}", uid, event.address(), event.replyAddress());
                return;
            }
            log.debug("RedisQues got notification for trim queue '{}'", queueName);
            QueueProcessingState queueProcessingState = myQueues.get(queueName);
            if (queueProcessingState == null) {
                log.trace("RedisQues Queue {} is handed by other consumer", queueName);
                return;
            }
            QueueState state = queueProcessingState.getState();
            log.trace("RedisQues consumer: {} queue: {} state: {}", uid, queueName, state);
            if (state != QueueState.CONSUMING) {
                //not in state consuming, trim now
                redisProvider.redis().onSuccess(redisAPI -> trimQueueItemIfNeeded(redisAPI, queueName))
                        .onFailure(throwable -> log.error("Redis: Unable to get redis api", throwable));
            } else {
                log.debug("RedisQues Queue {} is state of consuming, trim will process while consuming", queueName);
            }
        });
        registerMetricsGathering(configuration);
    }

    public void trimRequestConsumerUnregister(Handler<AsyncResult<Void>> handler) {
        trimRequestConsumer.unregister(handler);
    }


    private void registerMetricsGathering(RedisquesConfiguration configuration) {
        if (!configuration.getRedisMonitoringEnabled()) {
            return;
        }

        String metricsAddress = configuration.getPublishMetricsAddress();
        if (Strings.isNullOrEmpty(metricsAddress)) {
            return;
        }
        String metricStorageName = configuration.getMetricStorageName();
        int metricRefreshPeriod = configuration.getMetricRefreshPeriod();

        redisMonitor = new RedisMonitor(vertx, redisService, metricsAddress, metricStorageName, metricRefreshPeriod);
        redisMonitor.start();
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
                    queueRegistryService.checkQueues().onFailure(ex -> {
                        if (log.isWarnEnabled()) {
                            log.warn("TODO error handling", exceptionFactory.newException(ex));
                        }
                    });
                    return;
                case reset:
                    queueRegistryService.resetConsumers();
                    return;
                case stop:
                    queueRegistryService.gracefulStop(aVoid -> {/*no-op*/});
                    return;
            }

            // handle queue action operations
            queueActionsService.handle(queueOperation, event);
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
        queueRegistryService.stop();
        if(redisMonitor != null) {
            redisMonitor.stop();
            redisMonitor = null;
        }
    }

    public Future<Void> consume(final String queueName) {
        final Promise<Void> promise = Promise.promise();
        log.debug("RedisQues Requested to consume queue {}", queueName);
        queueRegistryService.refreshRegistration(queueName, event -> {
            if (event.failed()) {
                log.warn("Failed to refresh registration for queue '{}'.", queueName, event.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            queueMetrics.perQueueMetricsRefresh(queueName);
            // Make sure that I am still the registered consumer
            final String consumerKey = consumersPrefix + queueName;
            log.trace("RedisQues consume get: {}", consumerKey);
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(consumerKey, event1 -> {
                        if (event1.failed()) {
                            log.error("Unable to get consumer for queue {}", queueName, event1.cause());
                            return;
                        }
                        String consumer = Objects.toString(event1.result(), "");
                        log.trace("RedisQues refresh registration consumer: {}", consumer);
                        if (uid.equals(consumer)) {
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
                                trimQueueItemIfNeeded(redisAPI, queueName).onComplete(event22 -> {
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
                            queueRegistryService.notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
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

    /**
     * trim the queue items, if limit set. this function will always return as a succeededFuture.
     *
     * @param redisAPI
     * @param queueName
     * @return Future
     */
    private Future<Void> trimQueueItemIfNeeded(RedisAPI redisAPI, final String queueName) {
        final Promise<Void> promise = Promise.promise();
        final String queueKey = queuesPrefix + queueName;
        final QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
        // trim items if limits set
        if (queueConfiguration != null && queueConfiguration.getMaxQueueEntries() > 0) {
            final int maxQueueEntries = queueConfiguration.getMaxQueueEntries();
            log.debug("RedisQues Max queue entries {} found for queue {}", maxQueueEntries, queueName);
            redisAPI.ltrim(queueKey, "-" + maxQueueEntries, "-1").onComplete(ltrimResponse -> {
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
                        queueStatsService.createDequeueStatisticIfMissing(queueName);
                        queueStatsService.dequeueStatisticSetLastDequeueAttemptTimestamp(queueName, System.currentTimeMillis());
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
                                    queueMetrics.dequeueCounterIncrement();
                                    log.debug("RedisQues Message removed, queue {} is ready again", queueName);
                                    setMyQueuesState(queueName, QueueState.READY);

                                    Handler<Void> nextMsgHandler = event -> {
                                        // Issue notification to consume next message if any
                                        log.trace("RedisQues read queue: {}", queueKey);
                                        redisAPI.llen(queueKey, answer1 -> {
                                            if (answer1.succeeded() && answer1.result() != null && answer1.result().toInteger() > 0) {
                                                queueRegistryService.notifyConsumer(queueName).onComplete(event1 -> {
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
                                        queueRegistryService.unregisterConsumers(UnregisterConsumerType.GRACEFUL).onComplete(event -> {
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
                                log.trace("RedisQues will re-send the message to queue '{}' in {} seconds", queueName, retryInterval);
                                rescheduleSendMessageAfterFailure(queueName, retryInterval);
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
                })).onFailure(throwable -> {
                    // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    log.warn("Redis: Error on readQueue", throwable);
                    setMyQueuesState(queueName, QueueState.READY);
                    promise.complete();
                });
            } else {
                log.debug("Got a request to consume from locked queue {}", queueName);
                setMyQueuesState(queueName, QueueState.READY);
                promise.complete();
            }
        });
        return promise.future();
    }

    private void rescheduleSendMessageAfterFailure(final String queueName, int retryInSeconds) {
        log.trace("RedsQues reschedule after failure for queue: {}", queueName);

        vertx.setTimer(retryInSeconds * 1000L, timerId -> {
            queueStatsService.dequeueStatisticSetNextDequeueDueTimestamp(queueName, retryInSeconds * 1000L);
            if (log.isDebugEnabled()) {
                log.debug("RedisQues re-notify the consumer of queue '{}' at {}", queueName, new Date(System.currentTimeMillis()));
            }
            queueRegistryService.notifyConsumer(queueName).onComplete(event -> {
                if (event.failed()) {
                    log.warn("TODO error handling", exceptionFactory.newException(
                            "notifyConsumer(" + queueName + ") failed", event.cause()));
                }
                // reset the queue state to be consumed by {@link RedisQues#consume(String)}
                setMyQueuesState(queueName, QueueState.READY);
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
                    if (success) {
                        queueStatsService.dequeueStatisticSetLastDequeueSuccessTimestamp(queue,System.currentTimeMillis());
                        queueStatsService.dequeueStatisticSetNextDequeueDueTimestamp(queue,null);
                    }
                } else {
                    log.info("RedisQues QUEUE_ERROR: Consumer failed {} queue: {}",
                            uid, queue, exceptionFactory.newException(reply.cause()));
                    success = Boolean.FALSE;
                }


                handler.handle(success);
            });
            queueRegistryService.updateTimestamp(queue, null);
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

    public void setMyQueuesState(String queueName, QueueState state) {
        myQueues.compute(queueName, (s, queueProcessingState) -> {
            if (null == queueProcessingState) {
                // not in our list yet
                return new QueueProcessingState(state, 0);
            } else {
                if (queueProcessingState.getState() == QueueState.CONSUMING && state == QueueState.READY) {
                    // update the state and the timestamp when we change from CONSUMING to READY
                    return new QueueProcessingState(QueueState.READY, currentTimeMillis());
                } else {
                    // update the state but leave the timestamp unchanged
                    queueProcessingState.setState(state);
                    return queueProcessingState;
                }
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

    public Map<QueueState, Long> getQueueStateCount() {
        return myQueues.values().stream()
                .map(QueueProcessingState::getState)
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        () -> new EnumMap<>(QueueState.class),
                        Collectors.counting()
                ));
    }
}
