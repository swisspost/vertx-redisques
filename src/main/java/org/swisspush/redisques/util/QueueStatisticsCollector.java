package org.swisspush.redisques.util;

import static org.swisspush.redisques.util.RedisquesAPI.*;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.swisspush.redisques.lua.LuaScriptManager;

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

    private final RedisAPI redisAPI;
    private final LuaScriptManager luaScriptManager;
    private final String queuePrefix;

    public QueueStatisticsCollector(RedisAPI redisAPI, LuaScriptManager luaScriptManager,
        String queuePrefix) {
        this.redisAPI = redisAPI;
        this.luaScriptManager = luaScriptManager;
        this.queuePrefix = queuePrefix;
    }

    /**
     * Does reset all statistics values of the given queue. In memory but as well the persisted
     * ones in redis.
     *
     * Note: The reset is only executed on the persisted statistics data if there is really
     * a need for.
     *
     * @param queueName The queue name for which the statistic values must be reset.
     */
    public void resetQueueStatistics(String queueName) {
        AtomicLong failureCount = queueFailureCount.remove(queueName);
        queueSlowDownTime.remove(queueName);
        queueBackpressureTime.remove(queueName);
        if (failureCount != null && failureCount.get()>0) {
          // there was a real failure before, therefore we will execute this
          // cleanup as well on Redis itself as we would like to do redis operations
          // only if necessary of course.
          updateStatisticsInRedis(queueName);
        }
    }

    /**
     * Does reset all statistics values of all given queues. In memory but as well the persisted
     * ones in redis.
     * @param queues The list of queue names for which the statistic values must be reset.
     */
    public void resetQueueStatistics(JsonArray queues) {
        if (queues == null || queues.isEmpty()) {
            return;
        }
        final int size = queues.size();
        List<String> queueKeys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            resetQueueStatistics(queues.getString(i));
        }
    }

    /**
     * Increments the failure counter for the given queue by 1.
     *
     * Note: There is explicitely no decrement operation foreseen because once a queue
     * is operable again, it will reset the failure counter immediately to 0. Therefore only the
     * reset operation is needed in general.
     *
     * @param queueName The name of the queue for which the incrementation must be done.
     * @return The new value of the counter after the incrementation.
     */
    public long incrementQueueFailureCount(String queueName) {
        AtomicLong failureCount = queueFailureCount.putIfAbsent(queueName, new AtomicLong(1));
        updateStatisticsInRedis(queueName);
        if (failureCount != null) {
            return failureCount.addAndGet(1);
        }
        return 1;
    }

    /**
     * Retrieves the current failure count we have in memory for this redisques instance (there
     * might be more than one in use).
     *
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
     * @param queueName The name of the queue for which the value must be set.
     * @param time The backpressure time in ms
     */
    public void setQueueBackPressureTime(String queueName, long time) {
        if (time > 0) {
            queueBackpressureTime.put(queueName, time);
            updateStatisticsInRedis(queueName);
        } else {
            Long lastTime = queueBackpressureTime.remove(queueName);
            if (lastTime!=null){
                updateStatisticsInRedis(queueName);
            }
        }
    }

    /**
     * Retrieves the current used backpressure time we have in memory for this redisques instance.
     *
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
     * @param queueName The name of the queue for which the value must be set.
     * @param time The slowdown time in ms
     */
    public void setQueueSlowDownTime(String queueName, long time) {
        if (time > 0) {
            queueSlowDownTime.put(queueName, time);
            updateStatisticsInRedis(queueName);
        } else {
            Long lastTime = queueSlowDownTime.remove(queueName);
            if (lastTime!=null){
                updateStatisticsInRedis(queueName);
            }
        }
    }

    /**
     * Retrieves the current used slowdown time we have in memory for this redisques instance.
     *
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
     * Write all the collected statistics for the given Queue to
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
            redisAPI.hset(List.of(STATSKEY, queueName, obj.toString()), emptyHandler -> {
            });
        } else {
            redisAPI.hdel(List.of(STATSKEY, queueName), emptyHandler -> {
            });
        }
    }

    /**
     * An internally used class for statistics value storage per queue.
     */
    private class QueueStatistic {

        private final String queueName;
        private long size;
        private long failures;
        private long backpressureTime;
        private long slowdownTime;

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

        JsonObject getAsJsonObject() {
            return new JsonObject()
                .put(MONITOR_QUEUE_NAME, queueName)
                .put(MONITOR_QUEUE_SIZE, size)
                .put(STATISTIC_QUEUE_FAILURES, failures)
                .put(STATISTIC_QUEUE_BACKPRESSURE, backpressureTime)
                .put(STATISTIC_QUEUE_SLOWDOWN, slowdownTime);
        }
    }


    /**
     * Retrieve the queue statistics for the requested queues.
     *
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
        // first retrieve all queue sizes from the queues itself by help of the LUA script
        // which returns for each requested queueName the corresponding queue size.
        // Note: If a queue doesn't exists, it will anyway return 0 for the same, therefore
        //       the size of the returned queues must be equal in any case and has the same
        //       order as the requested queues.
        List<String> queueKeys = queues.stream().map(queue -> queuePrefix + queue).collect(
            Collectors.toList());
        luaScriptManager.handleMultiListLength(queueKeys, queueListLength -> {
            if (queueListLength == null) {
                log.error("Unexepected queue MultiListLength result null");
                event.reply(new JsonObject().put(STATUS, ERROR));
                return;
            }
            if (queueListLength.size() != queues.size()) {
                log.error("Unexpected queue MultiListLength result with unequal size {} : {}",
                    queues.size(), queueListLength.size());
                event.reply(new JsonObject().put(STATUS, ERROR));
                return;
            }
            // populate the list of queue statistics in a Hashmap for later fast merging
            final HashMap<String, QueueStatistic> statisticsMap = new HashMap<>();
            for (int i = 0; i < queues.size(); i++) {
                QueueStatistic qs = new QueueStatistic(queues.get(i));
                qs.setSize(queueListLength.get(i));
                statisticsMap.put(qs.queueName, qs);
            }
            // now retrieve all available statistics from Redis and merge them
            // together with the previous populated queue statistics map
            redisAPI.hvals(STATSKEY, statisticsSet -> {
                if (statisticsSet == null) {
                    log.error("Unexepected statistics queue evaluation result result null");
                    event.reply(new JsonObject().put(STATUS, ERROR));
                    return;
                }
                // put the received statistics data to the former prepared statistics objects
                // per queue
                Iterator<Response> itr = statisticsSet.result().stream().iterator();
                while (itr.hasNext()){
                    JsonObject jObj = new JsonObject(itr.next().toString());
                    String queueName = jObj.getString(RedisquesAPI.QUEUENAME);
                    QueueStatistic queueStatistic = statisticsMap.get(queueName);
                    if (queueStatistic != null) {
                        // if it isn't there, there was obviously no statistic available for this
                        queueStatistic.setFailures(jObj.getLong(QUEUE_FAILURES, 0L));
                        queueStatistic.setBackpressureTime(jObj.getLong(QUEUE_BACKPRESSURE, 0L));
                        queueStatistic.setSlowdownTime(jObj.getLong(QUEUE_SLOWDOWNTIME, 0L));
                    }
                }
                // build the final resulting statistics list from the former merged queue
                // values from various sources
                JsonArray result = new JsonArray();
                for (int i = 0; i < queues.size(); i++) {
                    String queueName = queues.get(i);
                    QueueStatistic stats = statisticsMap.get(queueName);
                    if (stats != null) {
                        result.add(stats.getAsJsonObject());
                    }
                }
                event.reply(new JsonObject().put(RedisquesAPI.STATUS, RedisquesAPI.OK)
                    .put(RedisquesAPI.QUEUES, result));
            });
        });
    }


}
