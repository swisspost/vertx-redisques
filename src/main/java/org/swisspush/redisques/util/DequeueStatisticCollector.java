package org.swisspush.redisques.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
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
        sharedData.getLock(DEQUEUE_STATISTIC_LOCK_PREFIX.concat(queueName)).onSuccess(lock ->
                sharedData.getAsyncMap(DEQUEUE_STATISTIC_DATA, (Handler<AsyncResult<AsyncMap<String, DequeueStatistic>>>) asyncResult -> {
                    if (asyncResult.failed()) {
                        log.error("Failed to get dequeue statistic data map.", asyncResult.cause());
                        lock.release();
                        return;
                    }
                    AsyncMap<String, DequeueStatistic> asyncMap = asyncResult.result();

                    asyncMap.get(queueName).onSuccess(sharedDequeueStatistic -> {
                        if (sharedDequeueStatistic == null) {
                            try {
                                asyncMap.put(queueName, dequeueStatistic).onSuccess(event -> {
                                    log.debug("dequeue statistic for queue {} added.", queueName);
                                    lock.release();
                                }).onFailure(event -> {
                                    log.debug("dequeue statistic for queue {} failed to add.", queueName);
                                    lock.release();
                                });
                            } catch (Exception throwable) {
                                log.error("Failed to pur dequeue statistic data for queue {}.", queueName, throwable);
                                lock.release();
                            }
                            return;
                        } else if (sharedDequeueStatistic.getLastUpdatedTimestamp() < dequeueStatistic.getLastUpdatedTimestamp()) {
                            if (dequeueStatistic.isMarkToDelete()) {
                                // delete
                                asyncMap.remove(queueName).onComplete(dequeueStatisticAsyncResult -> {
                                    log.debug("dequeue statistic for queue {} removed.", queueName);
                                    lock.release();
                                });
                            } else {
                                // update
                                asyncMap.put(queueName, dequeueStatistic).onComplete(event -> {
                                    if (event.succeeded()) {
                                        log.debug("dequeue statistic for queue {} updated.", queueName);
                                    } else {
                                        log.debug("dequeue statistic for queue {} failed to update.", queueName);
                                    }
                                    lock.release();
                                });
                            }
                            return;
                        } else {
                            log.debug("dequeue statistic for queue {} has newer data, update skipped", queueName);
                        }
                        lock.release();
                    }).onFailure(throwable -> {
                        lock.release();
                        log.error("Failed to get dequeue statistic data for queue {}.", queueName, throwable);
                    });

                })).onFailure(throwable -> {
            log.error("Failed to lock dequeue statistic data for queue {}.", queueName, throwable);
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
