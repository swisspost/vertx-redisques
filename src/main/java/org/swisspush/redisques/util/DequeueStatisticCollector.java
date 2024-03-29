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


    public void setDequeueStatistic(final String queueName, final DequeueStatistic newDequeueStatistic) {
        // To make lock works with Cluster, need set SemaphoreConfig.InitialPermits=1
        // Ref: https://github.com/vert-x3/vertx-hazelcast/blob/master/src/main/asciidoc/index.adoc#using-an-existing-hazelcast-cluster
        sharedData.getLock(DEQUEUE_STATISTIC_LOCK_PREFIX.concat(queueName)).onSuccess(lock ->
                sharedData.getAsyncMap(DEQUEUE_STATISTIC_DATA, (Handler<AsyncResult<AsyncMap<String, DequeueStatistic>>>) asyncResult -> {
                    if (asyncResult.failed()) {
                        log.error("Failed to get dequeue statistic data map.", asyncResult.cause());
                        lock.release();
                    } else {
                        AsyncMap<String, DequeueStatistic> asyncMap = asyncResult.result();
                        if (newDequeueStatistic == null) {
                            // remove
                            asyncMap.remove(queueName).onComplete(event -> lock.release());
                        } else {
                            // add or update
                            asyncMap.get(queueName).onSuccess(dequeueStatistic -> {
                                if (dequeueStatistic == null) {
                                    try {
                                        asyncMap.put(queueName, newDequeueStatistic).onSuccess(event -> {
                                            log.debug("dequeue statistic for queue {} added.", queueName);
                                            lock.release();
                                        }).onFailure(event -> {
                                            log.debug("dequeue statistic for queue {} failed to add.", queueName);
                                            lock.release();
                                        });
                                    } catch (Exception e) {
                                        lock.release();
                                    }
                                    return;
                                } else if (dequeueStatistic.getLastUpdatedTimestamp() < newDequeueStatistic.getLastUpdatedTimestamp()) {
                                    asyncMap.put(queueName, newDequeueStatistic).onComplete(event -> {
                                        if (event.succeeded()) {
                                            log.debug("dequeue statistic for queue {} updated.", queueName);
                                        } else {
                                            log.debug("dequeue statistic for queue {} failed to update.", queueName);
                                        }
                                        lock.release();
                                    });
                                    return;
                                } else {
                                    log.debug("dequeue statistic for queue {} has newer data, update skipped", queueName);
                                }
                                lock.release();
                            }).onFailure(throwable -> {
                                lock.release();
                                log.error("Failed to get dequeue statistic data for queue {}.", queueName, throwable);
                            });
                        }
                    }
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
            } else {
                AsyncMap<String, DequeueStatistic> asyncMap = asyncResult.result();
                asyncMap.entries().onSuccess(promise::complete).onFailure(throwable -> {
                    log.error("Failed to get dequeue statistic map", throwable);
                    promise.fail(throwable);
                });
            }
        });
        return promise.future();
    }
}
