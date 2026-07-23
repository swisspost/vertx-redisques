package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.NumberType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_NAME;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_SIZE;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;
import static org.swisspush.redisques.util.RedisquesAPI.STATISTIC_QUEUE_BACKPRESSURE;
import static org.swisspush.redisques.util.RedisquesAPI.STATISTIC_QUEUE_FAILURES;
import static org.swisspush.redisques.util.RedisquesAPI.STATISTIC_QUEUE_SLOWDOWN;
import static org.swisspush.redisques.util.RedisquesAPI.STATISTIC_QUEUE_SPEED;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

/**
 * Class StatisticsCollector helps collecting statistics information about queue handling and
 * failures.
 * <p>
 * Due to the fact that there is a Redisques responsible for one queue instance at the same time
 * in a cluster, we could assume that the values are good enough when cached locally for queue
 * processing by itself. If the responsible Redisques Instance changes, it will build up the
 * statistics straight away by itself anyway.
 * <p>
 * But in the case of the statistics read operation, we must get the statistics of all queues
 * existing, not only the ones which are treated by the redisques instance (there might be multiple
 * involved). Therefore, we must write the statistics values as well to redis and retrieve them from
 * there once needed. Note that the statistics is written asynch and only in the case of queue
 * failures, therefore it shouldn't happen too often.
 */
public class QueueStatisticsCollector {

    private static final Logger log = LoggerFactory.getLogger(QueueStatisticsCollector.class);

    private static final String STATSKEY = "redisques:stats";
    private final static String QUEUE_FAILURES = "failures";
    private final static String QUEUE_BACKPRESSURE = "backpressureTime";
    private final static String QUEUE_SLOWDOWNTIME = "slowdownTime";
    private final static long QUEUE_STATES_UPDATED_WITHIN_MS = 30_000L;

    private final Map<String, AtomicLong> queueFailureCount = new HashMap<>();
    private final Map<String, Long> queueBackpressureTime = new HashMap<>();
    private final Map<String, Long> queueSlowDownTime = new HashMap<>();
    private final Map<String, AtomicLong> queueMessageSpeedCtr = new ConcurrentHashMap<>();
    private final RedisquesConfigurationProvider configurationProvider;
    private final KeyspaceHelper keyspaceHelper;
    private volatile Map<String, Long> queueMessageSpeed = new HashMap<>();
    private final RedisService redisService;
    private final String queuePrefix;
    private final Vertx vertx;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final Semaphore redisRequestQuota;
    private final UpperBoundParallel upperBoundParallel;
    private Long speedStatisticsTimerId;


    public QueueStatisticsCollector(
            RedisService redisService,
            KeyspaceHelper keyspaceHelper,
            Vertx vertx,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisRequestQuota,
            int speedIntervalSec,
            RedisquesConfigurationProvider configurationProvider) {
        this.redisService = redisService;
        this.queuePrefix = keyspaceHelper.getQueuesPrefix();
        this.vertx = vertx;
        this.exceptionFactory = exceptionFactory;
        this.redisRequestQuota = redisRequestQuota;
        this.upperBoundParallel = new UpperBoundParallel(vertx, exceptionFactory);
        this.configurationProvider = configurationProvider;
        this.keyspaceHelper = keyspaceHelper;

        speedStatisticsScheduler(speedIntervalSec);
    }

    /**
     * Stops the QueueStatisticsCollector by unregistering the EventBus consumer and cancelling
     * the periodic speed statistics timer. This method should be called during shutdown to prevent
     * stale EventBus subscriptions in clustered environments.
     */
    public void stop() {
        log.debug("Stopping QueueStatisticsCollector");
        if (speedStatisticsTimerId != null) {
            boolean cancelled = vertx.cancelTimer(speedStatisticsTimerId);
            if (cancelled) {
                log.debug("Successfully cancelled speed statistics timer");
            } else {
                log.warn("Failed to cancel speed statistics timer with id: {}", speedStatisticsTimerId);
            }
        }
    }

    /**
     * This scheduled task collects and calculates the queue speed per queue in the local system
     * within the past time interval.
     * <p>
     * NOTE: The speed values are held in memory only due to performance reasons. We don't want to
     * store regularly a lot of values (all queues) for speed statistics only.
     * This would no more work properly of course if there are multiple Redisques instances deployed.
     * In such case, the client interested in such values must query each instance for itself.
     * <p>
     * By default, the speed interval is configured to 1 Minute (60'000ms) currently.
     */
    private void speedStatisticsScheduler(int speedIntervalSec) {
        if (speedIntervalSec <= 0) {
            // no speed statistics required
            log.debug("No speed statistics required");
            return;
        }
        this.speedStatisticsTimerId = vertx.setPeriodic(speedIntervalSec * 1000L, timerId -> {
            log.debug("Schedule statistics queue speed collection");
            // remember the accumulated message counter as speed value for the previous
            // speed measurement interval
            Map<String, Long> newQueueMessageSpeed = new HashMap<>();
            Iterator<Entry<String, AtomicLong>> itr = queueMessageSpeedCtr.entrySet().iterator();
            while (itr.hasNext()) {
                Entry<String, AtomicLong> entry = itr.next();
                if (entry.getValue().longValue() > 0) {
                    // The ctr was incremented within the previous speed measurement interval.
                    // Add the value to the speed map for later retrieval
                    newQueueMessageSpeed.put(entry.getKey(), entry.getValue().longValue());
                }
                // Clear the speed message counter in order to start again from scratch
                // for next interval.
                itr.remove();
            }
            // now we exchange the list to the new one and make the new speed values available
            // for speed retrievals about the previous measurement interval
            queueMessageSpeed = newQueueMessageSpeed;
            // Note: if we want to have this map persistently available over multiple redisques
            // instances, we would have to store it here after each interval on Redis in an appropriate
            // structure (map of queue-speed). Be aware of the fact that in a real world system the
            // list of queues with speed collection might be huge within a short amount of time
            // due to the exorbitant high queue dispatching performance of gateleen.
            // For the moment this is not yet implemented in order to keep system performance used
            // for queue speed evaluation as low as possible.
        });
    }

    /**
     * Does reset all failure statistics values of the given queue. In memory but as well the persisted
     * ones in redis.
     * <p>
     * Note: The reset is only executed on the persisted statistics data if there is really
     * a need for.
     *
     * @param queueName The queue name for which the statistic values must be reset.
     */
    public Future<Void> resetQueueFailureStatistics(String queueName) {
        Promise<Void> promise = Promise.promise();
        try {
            AtomicLong failureCount = queueFailureCount.remove(queueName);
            queueSlowDownTime.remove(queueName);
            queueBackpressureTime.remove(queueName);
            if (failureCount != null && failureCount.get() > 0) {
                // there was a real failure before, therefore we will execute this
                // cleanup as well on Redis itself as we would like to do redis operations
                // only if necessary of course.
                updateStatisticsInRedis(queueName).onComplete(event -> {
                    if (event.failed()) {
                        promise.fail(event.cause());
                    } else {
                        promise.complete();
                    }
                });
            } else {
                vertx.runOnContext(nonsense -> promise.complete());
            }
        } catch (Exception ex) {
            promise.fail(ex);
        }
        return promise.future();
    }

    /**
     * Does reset all failure statistics values of all given queues. In memory but as well the persisted
     * ones in redis.
     *
     * @param queues The list of queue names for which the statistic values must be reset.
     */
    public void resetQueueStatistics(JsonArray queues, BiConsumer<Throwable, Void> onDone) {
        if (queues == null || queues.isEmpty()) {
            onDone.accept(null, null);
            return;
        }
        final int redisRequestQuotaAcquireRetryTime = configurationProvider.configuration().getRedisMonitoringReqQuotaAcquireRetryTimeMs();
        upperBoundParallel.request(redisRequestQuota, redisRequestQuotaAcquireRetryTime, null, new UpperBoundParallel.Mentor<Void>() {
            int i = 0;
            final int size = queues.size();

            @Override
            public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void ctx) {
                String queueName = queues.getString(i++);
                resetQueueFailureStatistics(queueName).onComplete(event -> {
                    if (event.failed()) {
                        onDone.accept(event.cause(), null);
                    } else {
                        onDone.accept(null, null);
                    }
                });
                return i >= size;
            }

            @Override
            public boolean onError(Throwable ex, Void ctx) {
                return false;
            }

            @Override
            public void onDone(Void ctx) {
                onDone.accept(null, null);
            }
        });
    }

    /**
     * Signals a successful message distribution on the given queue.
     * Note: Increments the message counter for the given queue by 1.
     *
     * @param queueName The name of the queue for which success must be processed.
     */
    public void queueMessageSuccess(String queueName, BiConsumer<Throwable, Void> onDone) {
        // count the number of messages per queue for interval speed evaluation.
        AtomicLong messageCtr = queueMessageSpeedCtr.putIfAbsent(queueName, new AtomicLong(1));
        if (messageCtr != null) {
            messageCtr.incrementAndGet();
        }
        // whenever there is a message successfully sent, our failure statistics could be reset as well
        resetQueueFailureStatistics(queueName).onComplete(event -> {
            if (event.failed()) {
                onDone.accept(event.cause(), null);
            } else {
                onDone.accept(null, null);
            }
        });
    }

    /**
     * Retrieves the current queue speed in msg/time-unit for the requested queue
     * <p>
     * Note: The values are only given from local memory and therefore only for queue instances
     * managed by the corresonding Redisques instance.
     *
     * @param queueName The queue name for which we want to retrieve the measured speed
     * @return The latest queue speed (msg/ time-unit) for the given queue name
     */
    private long getQueueSpeed(String queueName) {
        Long speed = queueMessageSpeed.get(queueName);
        if (speed != null) {
            return speed;
        }
        return 0;
    }

    /**
     * Signals a failed message distribution on the given queue.
     * Increments the failure counter for the given queue by 1.
     * <p>
     * Note: There is explicitely no decrement operation foreseen because once a queue
     * is operable again, it will reset the failure counter immediately to 0. Therefore only the
     * reset operation is needed in general.
     *
     * @param queueName The name of the queue for which the incrementation must be done.
     * @return The new value of the counter after the incrementation.
     */
    public long queueMessageFailed(String queueName) {
        long newFailureCount = 1;
        AtomicLong failureCount = queueFailureCount.putIfAbsent(queueName, new AtomicLong(newFailureCount));
        if (failureCount != null) {
            newFailureCount = failureCount.addAndGet(1);
        }
        updateStatisticsInRedis(queueName).onComplete(event -> {
            if (event.failed()) {
                log.warn("failed to update statistics", event.cause());
            }
        });
        return newFailureCount;
    }

    /**
     * Retrieves the current failure count we have in memory for this redisques instance (there
     * might be more than one in use).
     * <p>
     * Note: Because each queue is associated resp. registered with a certain redisques instance, it
     * is valid to read this value just from our own memory only and no redis interaction takes
     * place.
     *
     * @param queueName The queue name for which we want to retrieve the current failure count
     * @return The evaluated failure count for the given queue name
     */
    public long getQueueFailureCount(String queueName) {
        AtomicLong count = queueFailureCount.get(queueName);
        if (count != null) {
            return count.longValue();
        }
        return 0;
    }

    /**
     * Set the statistics backpressure time to the given value. Note that this is done in memory
     * but as well persisted on redis.
     *
     * @param queueName The name of the queue for which the value must be set.
     * @param time      The backpressure time in ms
     */
    public void setQueueBackPressureTime(String queueName, long time) {
        if (time > 0) {
            queueBackpressureTime.put(queueName, time);
            updateStatisticsInRedis(queueName).onComplete(event -> {
                if (event.failed()) {
                    log.warn("failed to update statistics", event.cause());
                }
            });
        } else {
            Long lastTime = queueBackpressureTime.remove(queueName);
            if (lastTime != null) {
                updateStatisticsInRedis(queueName).onComplete(event -> {
                    if (event.failed()) {
                        log.warn("failed to update statistics", event.cause());
                    }
                });
            }
        }
    }

    /**
     * Retrieves the current used backpressure time we have in memory for this redisques instance.
     * <p>
     * Note: Because each queue is associated resp. registered with a certain redisques instance, it
     * is valid to read this value just from our own memory only and no redis interaction takes
     * place.
     *
     * @param queueName The queue name for which we want to retrieve the current failure count
     * @return The evaluated failure count for the given queue name
     */
    private long getQueueBackPressureTime(String queueName) {
        return queueBackpressureTime.getOrDefault(queueName, 0L);
    }

    /**
     * Set the statistics slowdown time to the given value. Note that this is done in memory
     * but as well persisted on redis.
     *
     * @param queueName The name of the queue for which the value must be set.
     * @param time      The slowdown time in ms
     */
    public void setQueueSlowDownTime(String queueName, long time) {
        if (time > 0) {
            queueSlowDownTime.put(queueName, time);
            updateStatisticsInRedis(queueName).onComplete(event -> {
                if (event.failed()) {
                    log.warn("failed to update statistics", event.cause());
                }
            });
        } else {
            Long lastTime = queueSlowDownTime.remove(queueName);
            if (lastTime != null) {
                updateStatisticsInRedis(queueName).onComplete(event -> {
                    if (event.failed()) {
                        log.warn("failed to update statistics", event.cause());
                    }
                });
            }
        }
    }

    /**
     * Retrieves the current used slowdown time we have in memory for this redisques instance.
     * <p>
     * Note: Because each queue is associated resp. registered with a certain redisques instance, it
     * is valid to read this value just from our own memory only and no redis interaction takes
     * place.
     *
     * @param queueName The queue name for which we want to retrieve the current failure count
     * @return The evaluated failure count for the given queue name
     */
    private long getQueueSlowDownTime(String queueName) {
        return queueSlowDownTime.getOrDefault(queueName, 0L);
    }

    /**
     * Write all the collected failure statistics for the given Queue to
     * redis for later usage if somebody requests the queue statistics.
     * If there are no valid useful data available eg. all 0, the corresponding
     * statistics entry is removed from redis
     */
    private Future<Void> updateStatisticsInRedis(String queueName) {
        Promise<Void> promise = Promise.promise();
        try {
            long failures = getQueueFailureCount(queueName);
            long slowDownTime = getQueueSlowDownTime(queueName);
            long backpressureTime = getQueueBackPressureTime(queueName);
            if (failures > 0 || slowDownTime > 0 || backpressureTime > 0) {
                JsonObject obj = new JsonObject();
                obj.put(QUEUENAME, queueName);
                obj.put(QUEUE_FAILURES, failures);
                obj.put(QUEUE_SLOWDOWNTIME, slowDownTime);
                obj.put(QUEUE_BACKPRESSURE, backpressureTime);
                redisService.hset(STATSKEY, queueName, obj.toString())
                        .onComplete(event -> {
                            if (event.failed()) {
                                promise.fail(exceptionFactory.newException("redisProvider.redis() failed", event.cause()));
                            } else {
                                promise.complete();
                            }
                        });
            } else {
                redisService.hdel(STATSKEY, queueName)
                        .onComplete(event -> {
                            if (event.failed()) {
                                promise.fail(exceptionFactory.newException("redisProvider.hdel() failed", event.cause()));
                            } else {
                                promise.complete();
                            }
                        });
            }
        } catch (RuntimeException ex) {
            promise.fail(ex);
        }
        return promise.future();
    }

    /**
     * get a queue item size which updated by queuecheck and queue runner, and synced by eventbus,
     * which not accurately reflects the current item quantity.
     *
     * @param queueName
     * @return approximate queueSize or 0 if not find
     */
    public Future<Long> getApproximateQueueSize(String queueName) {
        Promise<Long> promise = Promise.promise();

        getAllApproximateQueueSize().onComplete(event -> {
            if (event.failed()) {
                log.warn("Fail-open queue size fallback for '{}' (using size=0 because running-state collection failed)",
                        queueName, event.cause());
                promise.complete(0L);
                return;
            }
            promise.complete(event.result().getOrDefault(queueName, 0L));
        });

        return promise.future();
    }


    /**
     * Get all queues size from statistics
     *
     * @return
     */
    public Future<Map<String, Long>> getAllApproximateQueueSize() {
        return getAllApproximateQueueSize(0);
    }

    /**
     * Get all queues size from statistics, also allow user to filter how old the queue size.
     *
     * @param lastUpdateWithInMs only include queue states updated within this time window, in milliseconds, 0 mean all
     * @return
     */
    public Future<Map<String, Long>> getAllApproximateQueueSize(long lastUpdateWithInMs) {
        Promise<Map<String, Long>> promise = Promise.promise();
        JsonObject requestBody = RedisquesAPI.buildGetQueueRunningStates(lastUpdateWithInMs, 0, 0);
        vertx.eventBus().<JsonObject>request(keyspaceHelper.getAddress(), requestBody).onComplete(event -> {
            if (event.failed()) {
                log.error("failed to get the queue running states", event.cause());
                promise.fail(event.cause());
                return;
            }
            promise.complete(mergeQueueSizeFromAllQueueRunningStates(event.result().body().getJsonArray(RedisquesAPI.PAYLOAD)));
        });
        return promise.future();
    }

    /**
     * Merges the queue sizes reported by each redisques instance's queue running state into a single
     * per-queue size map, keeping only the most recently refreshed entry for each queue name.
     * <p>
     * The payload is expected to be a {@link JsonArray} of {@link JsonObject}s, one per instance, where
     * each entry maps a queue name to its running state (a {@link JsonObject} containing at least
     * {@code queueItemSizeCounter} and {@code lastRegisterRefreshedMillis}). For a given queue name that
     * appears in multiple instances, only the state with the highest {@code lastRegisterRefreshedMillis}
     * value is kept, and its {@code queueItemSizeCounter} is used as the merged size.
     *
     * Entries with malformed shape are ignored to stay robust during mixed-version cluster states.
     *
     * @param payload a {@link JsonArray} of per-instance {@link JsonObject}s mapping queue names to their running state
     * @return a {@link Map} of queue name to its merged (most recently updated) queue size
     */
    public static Map<String, Long> mergeQueueSizeFromAllQueueRunningStates(JsonArray payload) {
        Map<String, Long> merged = new HashMap<>();
        Map<String, Long> latestTs = new HashMap<>();
        if (payload == null || payload.isEmpty()) {
            return merged;
        }
        payload.stream().forEach(instanceObj -> {
            if (!(instanceObj instanceof JsonObject)) {
                log.warn("Ignoring malformed running-state instance payload entry of type '{}'",
                        instanceObj == null ? "null" : instanceObj.getClass().getName());
                return;
            }
            JsonObject instanceQueues = (JsonObject) instanceObj;
            instanceQueues.forEach(entry -> {
                if (!(entry.getValue() instanceof JsonObject)) {
                    log.warn("Ignoring malformed running-state queue entry for '{}': expected object but got '{}'",
                            entry.getKey(),
                            entry.getValue() == null ? "null" : entry.getValue().getClass().getName());
                    return;
                }
                JsonObject queueState = (JsonObject) entry.getValue();
                Long size = getLongOrDefault(queueState, "queueItemSizeCounter", 0L);
                Long lastRegisterRefreshedMillis = getLongOrDefault(queueState, "lastRegisterRefreshedMillis", 0L);
                if (size == null || lastRegisterRefreshedMillis == null) {
                    log.warn("Ignoring malformed running-state values for queue '{}': {}", entry.getKey(), queueState.encode());
                    return;
                }
                String name = entry.getKey();
                if (lastRegisterRefreshedMillis > latestTs.getOrDefault(name, Long.MIN_VALUE)) {
                    latestTs.put(name, lastRegisterRefreshedMillis);
                    merged.put(name, size);
                }
            });
        });
        return merged;
    }

    private static Long getLongOrDefault(JsonObject obj, String field, long defaultValue) {
        Object value = obj.getValue(field);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return null;
    }

    /**
     * An internally used class for statistics value storage per queue.
     */
    private static class QueueStatistic {

        private final String queueName;
        private long size;
        private long failures;
        private long backpressureTime;
        private long slowdownTime;
        private long speed;

        QueueStatistic(String queueName) {
            this.queueName = queueName;
        }

        void setSize(Long size) {
            if (size != null && size >= 0) {
                this.size = size;
                return;
            }
            this.size = 0;
        }

        void setFailures(Long failures) {
            if (failures != null && failures >= 0) {
                this.failures = failures;
                return;
            }
            this.failures = 0;
        }

        void setBackpressureTime(Long backpressureTime) {
            if (backpressureTime != null && backpressureTime >= 0) {
                this.backpressureTime = backpressureTime;
                return;
            }
            this.backpressureTime = 0;
        }

        void setSlowdownTime(Long slowdownTime) {
            if (slowdownTime != null && slowdownTime >= 0) {
                this.slowdownTime = slowdownTime;
                return;
            }
            this.slowdownTime = 0;
        }

        void setMessageSpeed(Long speed) {
            if (speed != null && speed >= 0) {
                this.speed = speed;
                return;
            }
            this.speed = 0;
        }

        JsonObject getAsJsonObject() {
            return new JsonObject()
                    .put(MONITOR_QUEUE_NAME, queueName)
                    .put(MONITOR_QUEUE_SIZE, size)
                    .put(STATISTIC_QUEUE_FAILURES, failures)
                    .put(STATISTIC_QUEUE_BACKPRESSURE, backpressureTime)
                    .put(STATISTIC_QUEUE_SLOWDOWN, slowdownTime)
                    .put(STATISTIC_QUEUE_SPEED, speed);
        }
    }

    /**
     * Retrieve the queue statistics for the requested queues.
     * <p>
     * Note: This operation does interact with Redis to retrieve the values for the statistics
     * for all queues requested (independent of the redisques instance for which the queues are
     * registered). Therefore this method must be used with care and not be called too often!
     *
     * @param queues           The queues for which we are interested in the statistics
     * @param includeQueueSize fetch queue item size
     * @return A Future
     */
    public Future<JsonObject> getQueueStatistics(final List<String> queues, final boolean includeQueueSize) {
        final Promise<JsonObject> promise = Promise.promise();
        if (queues == null || queues.isEmpty()) {
            log.debug("Queue statistics evaluation with empty queues, returning empty result");
            promise.complete(new JsonObject().put(STATUS, OK).put(RedisquesAPI.QUEUES, new JsonArray()));
            return promise.future();
        }
        final Map<String, Long> queueSizeFromStatistics = new HashMap<>();
        final Map<String, String> keyQueuePairs = new LinkedHashMap<>();
        final var ctx = new RequestCtx();
        ctx.includeQueueSize = includeQueueSize;
        for (String key : queues) {
            keyQueuePairs.put(queuePrefix + key, key);
        }
        Map<String, String> groupedKeyQueuePairs = RedisClusterUtil.groupMapBySlot(keyQueuePairs);

        if (includeQueueSize) {
            getAllApproximateQueueSize(QUEUE_STATES_UPDATED_WITHIN_MS).onComplete(event -> {
                if (event.failed()) {
                    log.warn("Fail-open queue-size fallback: running-state request failed, fetching missing queue sizes from Redis LLEN",
                            exceptionFactory.newException(event.cause()));
                } else {
                    queueSizeFromStatistics.putAll(event.result());
                }
                // Remove queues from statistics that don't exist in filtered queueKeys
                queueSizeFromStatistics.keySet().removeIf(queue -> !queues.contains(queue));
                ctx.keyQueuePair = new ArrayList<>(groupedKeyQueuePairs.entrySet());
                ctx.queueSizeFromStatistics = queueSizeFromStatistics;
                step1(ctx).compose(
                        nothing2 -> step2(ctx).compose(
                                nothing3 -> step3(ctx).compose(
                                        nothing4 -> step4(ctx))
                        )).onComplete(promise);
            });
        } else {
            ctx.keyQueuePair = new ArrayList<>(groupedKeyQueuePairs.entrySet());
            ctx.queueSizeFromStatistics = new HashMap<>();
            step1(ctx).compose(
                    nothing2 -> step2(ctx).compose(
                            nothing3 -> step3(ctx).compose(
                                    nothing4 -> step4(ctx))
                    )).onComplete(promise);
        }
        return promise.future();
    }

    /**
     * <p>Query queue lengths.</p>
     */
    Future<Void> step1(RequestCtx ctx) {
        if (!ctx.includeQueueSize) {
            return Future.succeededFuture();
        }
        int numQueues = ctx.keyQueuePair.size();
        log.debug("About to perform {} requests to redis just for monitoring", numQueues);
        long begRedisRequestsEpochMs = currentTimeMillis();

        assert ctx.queueLengths == null;
        // UpperBoundParallel may complete batches concurrently, therefore use a concurrent map.
        // Keying by queue name avoids relying on asynchronous completion order.
        ctx.queueLengths = new ConcurrentHashMap<>(ctx.keyQueuePair.size());

        var upperBoundPromise = Promise.<Void>promise();
        final int redisRequestQuotaAcquireRetryTime = configurationProvider.configuration().getRedisMonitoringReqQuotaAcquireRetryTimeMs();
        upperBoundParallel.request(redisRequestQuota, redisRequestQuotaAcquireRetryTime, ctx, new UpperBoundParallel.Mentor<>() {
            final Iterator<Entry<String, String>> queueKeyWithNameIter = ctx.keyQueuePair.iterator();

            @Override
            public boolean runOneMore(BiConsumer<Throwable, Void> onDone, RequestCtx ctx) {
                if (!queueKeyWithNameIter.hasNext()) {
                    return false;
                }

                List<Entry<String, String>> redisBatch = new ArrayList<>(RedisService.MAX_COMMANDS_IN_BATCH);
                while (queueKeyWithNameIter.hasNext() && redisBatch.size() < RedisService.MAX_COMMANDS_IN_BATCH) {
                    Entry<String, String> entry = queueKeyWithNameIter.next();
                    Long knownSize = ctx.queueSizeFromStatistics.get(entry.getValue());
                    if (knownSize != null) {
                        ctx.queueLengths.put(entry.getValue(), knownSize);
                    } else {
                        redisBatch.add(entry);
                    }
                }

                if (redisBatch.isEmpty()) {
                    onDone.accept(null, null);
                    return queueKeyWithNameIter.hasNext();
                }

                List<String> keyBatch = new ArrayList<>(redisBatch.size());
                for (Entry<String, String> entry : redisBatch) {
                    keyBatch.add(entry.getKey());
                }

                redisService.clusterSafeBatch(Command.LLEN, keyBatch, List.of())
                        .onSuccess(responses -> {
                            if (responses.size() != redisBatch.size()) {
                                onDone.accept(exceptionFactory.newException(
                                        "Unexpected queue length batch result with unequal size "
                                                + redisBatch.size() + " : " + responses.size()), null);
                                return;
                            }
                            for (int i = 0; i < responses.size(); i++) {
                                ctx.queueLengths.put(redisBatch.get(i).getValue(), responses.get(i).toLong());
                            }
                            onDone.accept(null, null);
                        })
                        .onFailure(ex -> onDone.accept(exceptionFactory.newException(ex), null));

                return queueKeyWithNameIter.hasNext();
            }

            @Override
            public boolean onError(Throwable ex, RequestCtx ctx) {
                upperBoundPromise.fail(exceptionFactory.newException(
                        "Unexpected queue length result", ex));
                return false;
            }

            @Override
            public void onDone(RequestCtx ctx) {
                upperBoundPromise.complete();
            }
        });

        return upperBoundPromise.future().compose(v -> {
            long durRedisRequestsMs = currentTimeMillis() - begRedisRequestsEpochMs;
            String fmt2 = "All those {} redis requests took {}ms";
            if (durRedisRequestsMs > 3000) {
                log.warn(fmt2, numQueues, durRedisRequestsMs);
            } else {
                log.debug(fmt2, numQueues, durRedisRequestsMs);
            }

            if (ctx.queueLengths.size() != ctx.keyQueuePair.size()) {
                return failedFuture(exceptionFactory.newException(
                        "Unexpected queue length result with unequal size "
                                + ctx.keyQueuePair.size() + " : " + ctx.queueLengths.size()));
            }
            return succeededFuture();
        });
    }

    /**
     * <p>init queue statistics.</p>
     */
    Future<Void> step2(RequestCtx ctx) {
        if (ctx.includeQueueSize) {
            assert ctx.queueLengths != null;
        }
        // populate the list of queue statistics in a Hashmap for later fast merging
        ctx.statistics = new HashMap<>(ctx.keyQueuePair.size());
        for (int i = 0; i < ctx.keyQueuePair.size(); i++) {
            QueueStatistic qs = new QueueStatistic(ctx.keyQueuePair.get(i).getValue());
            if (ctx.includeQueueSize) {
                qs.setSize(ctx.queueLengths.get(qs.queueName));
            }
            qs.setMessageSpeed(getQueueSpeed(qs.queueName));
            ctx.statistics.put(qs.queueName, qs);
        }
        return Future.succeededFuture();
    }

    /**
     * <p>retrieve all available failure statistics from Redis and merge them
     * together with the previous populated common queue statistics map</p>
     */
    Future<Void> step3(RequestCtx ctx) {
        assert ctx.statistics != null;
        return vertx.executeBlocking(executeBlockingPromise -> redisService.hvals(STATSKEY).onComplete(statisticsSet -> {
            if (statisticsSet == null || statisticsSet.failed()) {
                executeBlockingPromise.fail(new RuntimeException("statistics queue evaluation failed",
                        statisticsSet == null ? null : statisticsSet.cause()));
                return;
            }
            ctx.redisFailStats = statisticsSet.result();
            assert ctx.redisFailStats != null;
            executeBlockingPromise.complete();
        }), false);

    }

    /**
     * <p>put received statistics data to the former prepared statistics objects per
     * queue.</p>
     */
    Future<JsonObject> step4(RequestCtx ctx) {
        assert ctx.redisFailStats != null;
        return vertx.executeBlocking(executeBlockingPromise -> {
            for (Response response : ctx.redisFailStats) {
                JsonObject jObj = new JsonObject(response.toString());
                String queueName = jObj.getString(QUEUENAME);
                QueueStatistic queueStatistic = ctx.statistics.get(queueName);
                if (queueStatistic != null) {
                    // if it isn't there, there is obviously no statistic needed
                    queueStatistic.setFailures(jObj.getLong(QUEUE_FAILURES, 0L));
                    queueStatistic.setBackpressureTime(jObj.getLong(QUEUE_BACKPRESSURE, 0L));
                    queueStatistic.setSlowdownTime(jObj.getLong(QUEUE_SLOWDOWNTIME, 0L));
                }
            }
            // build the final resulting statistics list from the former merged queue
            // values from various sources
            JsonArray result = new JsonArray();
            for (Entry<String, String> queueKeyPair : ctx.keyQueuePair) {
                QueueStatistic stats = ctx.statistics.get(queueKeyPair.getValue());
                if (stats != null) {
                    result.add(stats.getAsJsonObject());
                }
            }
            executeBlockingPromise.complete(new JsonObject().put(STATUS, OK)
                    .put(RedisquesAPI.QUEUES, result));
        }, false);
    }

    /**
     * Retrieve the summarized queue speed for the requested queues.
     * <p>
     * Note: This method only treats queue speed values from the current redisques instance. There
     * is no persistent storage of these values due to performance reasons yet.
     *
     * @param event  The event on which we will answer finally
     * @param queues The queues for which we are interested in the overall speed
     */
    public void getQueuesSpeed(Message<JsonObject> event, final List<String> queues) {
        if (queues == null || queues.isEmpty()) {
            log.debug("No matching filtered queues given");
            event.reply(new JsonObject().put(STATUS, OK).put(STATISTIC_QUEUE_SPEED, 0L));
            return;
        }
        // loop over all queues and summarize the currently available speed values
        long speed = 0;
        for (String queue : queues) {
            speed = speed + getQueueSpeed(queue);
        }
        event.reply(new JsonObject().put(STATUS, OK).put(STATISTIC_QUEUE_SPEED, speed));
    }

    /**
     * <p>Holds intermediate state related to a {@link #getQueueStatistics(List, boolean)}}
     * request.</p>
     */
    private static class RequestCtx {
        private List<Map.Entry<String, String>> keyQueuePair; // Requested queues to analyze
        private Map<String, Long> queueSizeFromStatistics; // Queue sizes from statistics
        private Map<String, Long> queueLengths;
        private Boolean includeQueueSize;
        private HashMap<String, QueueStatistic> statistics; // Stats we're going to populate
        private Response redisFailStats; // failure stats we got from redis.
    }

}
