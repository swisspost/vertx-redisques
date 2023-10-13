package org.swisspush.redisques.util;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.NumberType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
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

    private final Map<String, AtomicLong> queueFailureCount = new HashMap<>();
    private final Map<String, Long> queueBackpressureTime = new HashMap<>();
    private final Map<String, Long> queueSlowDownTime = new HashMap<>();
    private final ConcurrentMap<String, AtomicLong> queueMessageSpeedCtr = new ConcurrentHashMap<>();
    private volatile Map<String, Long> queueMessageSpeed = new HashMap<>();
    private final RedisProvider redisProvider;
    private final String queuePrefix;
    private final Vertx vertx;

    public QueueStatisticsCollector(RedisProvider redisProvider,
                                    String queuePrefix, Vertx vertx, int speedIntervalSec) {
        this.redisProvider = redisProvider;
        this.queuePrefix = queuePrefix;
        this.vertx = vertx;
        speedStatisticsScheduler(speedIntervalSec);
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
        vertx.setPeriodic(speedIntervalSec * 1000L, timerId -> {
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
    public void resetQueueFailureStatistics(String queueName) {
        AtomicLong failureCount = queueFailureCount.remove(queueName);
        queueSlowDownTime.remove(queueName);
        queueBackpressureTime.remove(queueName);
        if (failureCount != null && failureCount.get() > 0) {
            // there was a real failure before, therefore we will execute this
            // cleanup as well on Redis itself as we would like to do redis operations
            // only if necessary of course.
            updateStatisticsInRedis(queueName);
        }
    }

    /**
     * Does reset all failure statistics values of all given queues. In memory but as well the persisted
     * ones in redis.
     *
     * @param queues The list of queue names for which the statistic values must be reset.
     */
    public void resetQueueStatistics(JsonArray queues) {
        if (queues == null || queues.isEmpty()) {
            return;
        }
        final int size = queues.size();
        for (int i = 0; i < size; i++) {
            resetQueueFailureStatistics(queues.getString(i));
        }
    }

    /**
     * Signals a successful message distribution on the given queue.
     * Note: Increments the message counter for the given queue by 1.
     *
     * @param queueName The name of the queue for which success must be processed.
     */
    public void queueMessageSuccess(String queueName) {
        // count the number of messages per queue for interval speed evaluation.
        AtomicLong messageCtr = queueMessageSpeedCtr.putIfAbsent(queueName, new AtomicLong(1));
        if (messageCtr != null) {
            messageCtr.incrementAndGet();
        }
        // whenever there is a message successfully sent, our failure statistics could be reset as well
        resetQueueFailureStatistics(queueName);
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
        updateStatisticsInRedis(queueName);
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
            updateStatisticsInRedis(queueName);
        } else {
            Long lastTime = queueBackpressureTime.remove(queueName);
            if (lastTime != null) {
                updateStatisticsInRedis(queueName);
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
            updateStatisticsInRedis(queueName);
        } else {
            Long lastTime = queueSlowDownTime.remove(queueName);
            if (lastTime != null) {
                updateStatisticsInRedis(queueName);
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
    private void updateStatisticsInRedis(String queueName) {
        long failures = getQueueFailureCount(queueName);
        long slowDownTime = getQueueSlowDownTime(queueName);
        long backpressureTime = getQueueBackPressureTime(queueName);
        if (failures > 0 || slowDownTime > 0 || backpressureTime > 0) {
            JsonObject obj = new JsonObject();
            obj.put(QUEUENAME, queueName);
            obj.put(QUEUE_FAILURES, failures);
            obj.put(QUEUE_SLOWDOWNTIME, slowDownTime);
            obj.put(QUEUE_BACKPRESSURE, backpressureTime);
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.hset(List.of(STATSKEY, queueName, obj.toString()),
                    emptyHandler -> {
                    })).onFailure(throwable -> log.error("Redis: Error in updateStatisticsInRedis", throwable));
        } else {
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.hdel(List.of(STATSKEY, queueName),
                    emptyHandler -> {
                    })).onFailure(throwable -> log.error("Redis: Error in updateStatisticsInRedis", throwable));

        }
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
     * @param event  The event on which we will answer finally
     * @param queues The queues for which we are interested in the statistics
     */
    public void getQueueStatistics(Message<JsonObject> event, final List<String> queues) {
        if (queues == null || queues.isEmpty()) {
            log.debug("Queue statistics evaluation with empty queues, returning empty result");
            event.reply(new JsonObject().put(STATUS, OK).put(RedisquesAPI.QUEUES, new JsonArray()));
            return;
        }
        redisProvider.connection().onSuccess(conn -> {
            List<Future> responses = queues.stream().map(queue -> conn.send(Request.cmd(Command.LLEN, queuePrefix + queue))
            ).collect(Collectors.toList());
            CompositeFuture.all(responses).onFailure(throwable -> {
                log.error("Unexepected queue length result");
                event.reply(new JsonObject().put(STATUS, ERROR));
            }).onSuccess(compositeFuture -> {
                List<NumberType> queueLengthList = compositeFuture.list();
                if (queueLengthList == null) {
                    log.error("Unexepected queue length result null");
                    event.reply(new JsonObject().put(STATUS, ERROR));
                    return;
                }
                if (queueLengthList.size() != queues.size()) {
                    log.error("Unexpected queue length result with unequal size {} : {}",
                            queues.size(), queueLengthList.size());
                    event.reply(new JsonObject().put(STATUS, ERROR));
                    return;
                }
                // populate the list of queue statistics in a Hashmap for later fast merging
                final HashMap<String, QueueStatistic> statisticsMap = new HashMap<>();
                for (int i = 0; i < queues.size(); i++) {
                    QueueStatistic qs = new QueueStatistic(queues.get(i));
                    qs.setSize(queueLengthList.get(i).toLong());
                    qs.setMessageSpeed(getQueueSpeed(qs.queueName));
                    statisticsMap.put(qs.queueName, qs);
                }
                // now retrieve all available failure statistics from Redis and merge them
                // together with the previous populated common queue statistics map
                redisProvider.redis().onSuccess(redisAPI -> redisAPI.hvals(STATSKEY, statisticsSet -> {
                    if (statisticsSet == null) {
                        log.error("Unexpected statistics queue evaluation result result null");
                        event.reply(new JsonObject().put(STATUS, ERROR));
                        return;
                    }
                    // put the received statistics data to the former prepared statistics objects
                    // per queue
                    for (Response response : statisticsSet.result()) {
                        JsonObject jObj = new JsonObject(response.toString());
                        String queueName = jObj.getString(RedisquesAPI.QUEUENAME);
                        QueueStatistic queueStatistic = statisticsMap.get(queueName);
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
                    for (String queueName : queues) {
                        QueueStatistic stats = statisticsMap.get(queueName);
                        if (stats != null) {
                            result.add(stats.getAsJsonObject());
                        }
                    }
                    event.reply(new JsonObject().put(RedisquesAPI.STATUS, RedisquesAPI.OK)
                            .put(RedisquesAPI.QUEUES, result));
                })).onFailure(throwable -> {
                    log.error("Redis: Error in getQueueStatistics", throwable);
                    event.reply(new JsonObject().put(STATUS, ERROR));
                });

            });
        }).onFailure(throwable -> {
            log.warn("Redis: Failed to get queue length.", throwable);
            event.reply(new JsonObject().put(STATUS, ERROR));
        });
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

}
