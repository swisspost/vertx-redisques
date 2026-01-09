package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DequeueStatisticCollector {
    private static final Logger log = LoggerFactory.getLogger(DequeueStatisticCollector.class);
    private final boolean dequeueStatisticEnabled;
    private final RedisService redisService;
    private final KeyspaceHelper keyspaceHelper;

    public DequeueStatisticCollector(boolean dequeueStatisticEnabled, RedisService redisService, KeyspaceHelper keyspaceHelper) {
        this.dequeueStatisticEnabled = dequeueStatisticEnabled;
        this.redisService = redisService;
        this.keyspaceHelper = keyspaceHelper;
    }


    public Future<Map<String, DequeueStatistic>> getAllDequeueStatistics() {
        // Check if dequeue statistics are enabled
        if (!dequeueStatisticEnabled) {
            return Future.succeededFuture(Collections.emptyMap()); // Return an empty map to avoid NullPointerExceptions
        }
        Promise<Map<String, DequeueStatistic>> promise = Promise.promise();

        redisService.hvals(keyspaceHelper.getDequeueStatisticKey()).onComplete(statisticsSet -> {
            if (statisticsSet == null || statisticsSet.failed()) {
                promise.fail(new RuntimeException("Redis: dequeue statistics queue evaluation failed",
                        statisticsSet == null ? null : statisticsSet.cause()));
            } else {
                Map<String, DequeueStatistic> result = new HashMap<>();
                for (Response response : statisticsSet.result()) {
                    JsonObject jsonObject = new JsonObject(response.toString());
                    DequeueStatistic dequeueStatistic = DequeueStatistic.fromJson(jsonObject);
                    if (dequeueStatistic != null) {
                        result.put(dequeueStatistic.getQueueName(), dequeueStatistic);
                    }
                }
                promise.complete(result);
            }
        });
        return promise.future();
    }

    /**
     *
     * @param queueName
     * @param dequeueStatistic
     * @return an always completed future
     */
    public Future<Void> setDequeueStatistic(String queueName, DequeueStatistic dequeueStatistic) {
        if (dequeueStatistic.isMarkedForRemoval()) {
            return removeDequeueStatistic(queueName);
        } else {
            return addOrUpdateDequeueStatistic(queueName, dequeueStatistic);
        }
    }

    private Future<Void> removeDequeueStatistic(String queueName) {
        Promise<Void> promise = Promise.promise();
        redisService.hdel(keyspaceHelper.getDequeueStatisticKey(), queueName).onComplete(event -> {
            if (event.failed()) {
                log.warn("failed to delete dequeueStatistic for {}.", queueName, event.cause().getMessage());
            }
            redisService.zrem(keyspaceHelper.getDequeueStatisticTsKey(), queueName).onComplete(event1 -> {
                if (event1.failed()) {
                    log.warn("failed to delete dequeueStatistic timestamp for {}.", queueName, event1.cause().getMessage());
                }
                promise.complete();
            });
        });
        return promise.future();
    }

    private Future<Void> addOrUpdateDequeueStatistic(String queueName, DequeueStatistic dequeueStatistic) {
        Promise<Void> promise = Promise.promise();
        //note: flag 'GT' is not supports in old Redis for ZADD, needed V >= 6.2.0
        redisService.zscore(keyspaceHelper.getDequeueStatisticTsKey(), queueName).onComplete(result -> {
            if (result.failed()) {
                log.warn("failed to get score of dequeueStatistic for {}. follow operations will skip", queueName);
                promise.complete();
                return;
            }
            if (result.result() == null || dequeueStatistic.getLastUpdatedTimestamp() > result.result().toLong()) {
                redisService.zadd(keyspaceHelper.getDequeueStatisticTsKey(), List.of("CH"),
                        queueName,
                        Double.toString(dequeueStatistic.getLastUpdatedTimestamp())).onComplete(zaddAsyncResult -> {
                    if (zaddAsyncResult.failed()) {
                        log.warn("Redis: Error in update DequeueStatistic Timestamp for queue {}", queueName, zaddAsyncResult.cause());
                        promise.complete();
                    } else {
                        if (zaddAsyncResult.result() != null && zaddAsyncResult.result().toInteger() == 1) {
                            redisService.hset(keyspaceHelper.getDequeueStatisticKey(), queueName, dequeueStatistic.asJson().encode()).onComplete(hsetAsyncResult -> {
                                if (hsetAsyncResult.failed()) {
                                    log.warn("Redis: Error in update DequeueStatistic for queue {}", queueName, hsetAsyncResult.cause());
                                }
                                promise.complete();
                            });
                        } else {
                            log.info("DequeueStatistic timestamp update failed {}, skip.", queueName);
                            promise.complete();
                        }
                    }
                });
            } else {
                log.debug("a newer dequeueStatistic for {} already exists", queueName);
                promise.complete();
            }
        });
        return promise.future();
    }
}
