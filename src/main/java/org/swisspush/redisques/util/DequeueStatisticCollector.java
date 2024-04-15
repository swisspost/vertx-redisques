package org.swisspush.redisques.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.shareddata.SharedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DequeueStatisticCollector {
    private static final Logger log = LoggerFactory.getLogger(DequeueStatisticCollector.class);
    private final static String DEQUEUE_STATISTIC_DATA = "dequeueStatisticData";
    private final static String DEQUEUE_STATISTIC_LOCK_PREFIX = "dequeueStatisticLock.";
    private final SharedData sharedData;

    public DequeueStatisticCollector(Vertx vertx) {
        this.sharedData = vertx.sharedData();
    }

    public void setDequeueStatistic(final String queueName, final DequeueStatistic dequeueStatistic) {
        sharedData.getLock(DEQUEUE_STATISTIC_LOCK_PREFIX.concat(queueName)).onComplete(lockAsyncResult -> {
            if (lockAsyncResult.failed()) {
                log.error("Failed to lock dequeue statistic data for queue {}.", queueName, lockAsyncResult.cause());
            } else {
                final Lock lock = lockAsyncResult.result();
                sharedData.getAsyncMap(DEQUEUE_STATISTIC_DATA, (Handler<AsyncResult<AsyncMap<String, DequeueStatistic>>>) asyncResult -> {
                    if (asyncResult.failed()) {
                        log.error("Failed to get shared dequeue statistic data map.", asyncResult.cause());
                        lock.release();
                        return;
                    }
                    AsyncMap<String, DequeueStatistic> asyncMap = asyncResult.result();
                    asyncMap.size().onComplete(mapSizeResult -> {
                        log.debug("shared dequeue statistic map size: {}", mapSizeResult.result());
                        asyncMap.get(queueName).onComplete(dequeueStatisticAsyncResult -> {
                            if (dequeueStatisticAsyncResult.failed()) {
                                lock.release();
                                log.error("Failed to get shared dequeue statistic data for queue {}.", queueName, dequeueStatisticAsyncResult.cause());
                            } else {
                                final DequeueStatistic sharedDequeueStatistic = dequeueStatisticAsyncResult.result();
                                if (sharedDequeueStatistic == null) {
                                    try {
                                        asyncMap.put(queueName, dequeueStatistic).onComplete(voidAsyncResult -> {
                                            if (voidAsyncResult.failed()){
                                                log.error("shared dequeue statistic for queue {} failed to add.", queueName, voidAsyncResult.cause());
                                            } else {
                                                log.debug("shared dequeue statistic for queue {} added.", queueName);
                                            }
                                            lock.release();
                                        });
                                    } catch (Exception exception) {
                                        log.error("Failed to put shared dequeue statistic for queue {}.", queueName, exception);
                                        lock.release();
                                    }
                                } else if (sharedDequeueStatistic.getLastUpdatedTimestamp() < dequeueStatistic.getLastUpdatedTimestamp()) {
                                    if (dequeueStatistic.isMarkedForRemoval()) {
                                        // delete
                                        asyncMap.remove(queueName).onComplete(removeAsyncResult -> {
                                            if (removeAsyncResult.failed()){
                                                log.error("failed to removed shared dequeue statistic for queue {}.", queueName, removeAsyncResult.cause());
                                            } else {
                                                log.debug("shared dequeue statistic for queue {} removed.", queueName);
                                            }
                                            lock.release();
                                        });
                                    } else {
                                        // update
                                        asyncMap.put(queueName, dequeueStatistic).onComplete(event -> {
                                            if (event.failed()) {
                                                log.error("shared dequeue statistic for queue {} failed to update.", queueName, event.cause());
                                            } else {
                                                log.debug("shared dequeue statistic for queue {} updated.", queueName);
                                            }
                                            lock.release();
                                        });
                                    }
                                } else {
                                    log.debug("shared dequeue statistic for queue {} has newer data, update skipped", queueName);
                                    lock.release();
                                }
                            }
                        });
                    });
                });
            }
        });
    }

    public Future<Map<String, DequeueStatistic>> getAllDequeueStatistics() {
        Promise<Map<String, DequeueStatistic>> promise = Promise.promise();
        sharedData.getAsyncMap(DEQUEUE_STATISTIC_DATA, (Handler<AsyncResult<AsyncMap<String, DequeueStatistic>>>) asyncResult -> {
            if (asyncResult.failed()) {
                log.error("Failed to get dequeue statistic data map.", asyncResult.cause());
                promise.fail(asyncResult.cause());
                return;
            }
            AsyncMap<String, DequeueStatistic> asyncMap = asyncResult.result();
            asyncMap.entries().onSuccess(promise::complete).onFailure(throwable -> {
                log.error("Failed to get dequeue statistic map", throwable);
                promise.fail(throwable);
            });

        });
        return promise.future();
    }
}
