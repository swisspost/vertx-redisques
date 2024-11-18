package org.swisspush.redisques;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.DequeueStatistic;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static java.lang.System.currentTimeMillis;

/**
 * Notify not-active/not-empty queues to be processed (e.g. after a reboot).
 * Check timestamps of not-active/empty queues.
 * This uses a sorted set of queue names scored by last update timestamp.
 */
public class QueueWatcherService {
    private static final Logger log = LoggerFactory.getLogger(QueueWatcherService.class);

    private final Vertx vertx;
    private final RedisquesConfigurationProvider configurationProvider;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final RedisProvider redisProvider;
    private final PeriodicSkipScheduler periodicSkipScheduler;
    private final UpperBoundParallel upperBoundParallel;
    private final QueueStatisticsCollector queueStatisticsCollector;
    private final String queuesPrefix;
    private final String queuesKey;
    private final String queueCheckLastexecKey;
    private final QueueRegistrationService queueRegistrationService;
    private final Map<String, DequeueStatistic> dequeueStatistic;
    private final Semaphore checkQueueRequestsQuota;
    private final boolean dequeueStatisticEnabled;

    QueueWatcherService(Vertx vertx, RedisquesConfigurationProvider configurationProvider, QueueRegistrationService queueRegistrationService,
                        RedisQuesExceptionFactory exceptionFactory, RedisProvider redisProvider,
                        String queuesKey, String queueCheckLastexecKey, String queuesPrefix,
                        QueueStatisticsCollector queueStatisticsCollector, Map<String, DequeueStatistic> dequeueStatistic,
                        boolean dequeueStatisticEnabled,
                        Semaphore checkQueueRequestsQuota) {
        this.vertx = vertx;
        this.configurationProvider = Objects.requireNonNull(configurationProvider);
        this.exceptionFactory = Objects.requireNonNull(exceptionFactory);
        this.redisProvider = Objects.requireNonNull(redisProvider);
        this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
        this.upperBoundParallel = new UpperBoundParallel(vertx, exceptionFactory);
        this.queueStatisticsCollector = Objects.requireNonNull(queueStatisticsCollector);
        this.queuesKey = queuesKey;
        this.queueCheckLastexecKey = queueCheckLastexecKey;
        this.queuesPrefix = queuesPrefix;
        this.queueRegistrationService = Objects.requireNonNull(queueRegistrationService);
        this.dequeueStatistic = Objects.requireNonNull(dequeueStatistic);
        this.dequeueStatisticEnabled = dequeueStatisticEnabled;
        this.checkQueueRequestsQuota = checkQueueRequestsQuota;
    }

    void registerQueueCheck() {
        periodicSkipScheduler.setPeriodic(configurationProvider.configuration().getCheckIntervalTimerMs(), "checkQueues", periodicEvent -> {
            redisProvider.redis().compose((RedisAPI redisAPI) -> {
                int checkInterval = configurationProvider.configuration().getCheckInterval();
                return redisAPI.send(Command.SET, queueCheckLastexecKey, String.valueOf(currentTimeMillis()), "NX", "EX", String.valueOf(checkInterval));
            }).compose((Response todoExplainWhyThisIsIgnored) -> {
                log.info("periodic queue check is triggered now");
                return checkQueues();
            }).onFailure((Throwable ex) -> {
                if (log.isErrorEnabled()) log.error("TODO error handling", exceptionFactory.newException(ex));
            });
        });
    }

    Future<Void> checkQueues() {
        final long startTs = System.currentTimeMillis();
        final var ctx = new Object() {
            long limit;
            RedisAPI redisAPI;
            AtomicInteger counter;
            Iterator<Response> iter;
        };
        return Future.<Void>succeededFuture().compose((Void v) -> {
            log.debug("Checking queues timestamps");
            // List all queues that look inactive (i.e. that have not been updated since 3 periods).
            ctx.limit = currentTimeMillis() - 3L * configurationProvider.configuration().getRefreshPeriod() * 1000;
            return redisProvider.redis();
        }).compose((RedisAPI redisAPI) -> {
            ctx.redisAPI = redisAPI;
            var p = Promise.<Response>promise();
            redisAPI.zrangebyscore(Arrays.asList(queuesKey, "-inf", String.valueOf(ctx.limit)), p);
            return p.future();
        }).compose((Response queues) -> {
            if (log.isDebugEnabled()) {
                log.debug("zrangebyscore time used is {} ms", System.currentTimeMillis() - startTs);
            }
            assert ctx.counter == null;
            assert ctx.iter == null;
            ctx.counter = new AtomicInteger(queues.size());
            ctx.iter = queues.iterator();
            log.trace("RedisQues update queues: {}", ctx.counter);
            var p = Promise.<Void>promise();
            upperBoundParallel.request(checkQueueRequestsQuota, null, new UpperBoundParallel.Mentor<Void>() {
                @Override
                public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Void ctx_) {
                    if (ctx.iter.hasNext()) {
                        final long perQueueStartTs = System.currentTimeMillis();
                        var queueObject = ctx.iter.next();
                        // Check if the inactive queue is not empty (i.e. the key exists)
                        final String queueName = queueObject.toString();
                        String key = queuesPrefix + queueName;
                        log.trace("RedisQues update queue: {}", key);
                        Handler<Void> refreshRegHandler = event -> {
                            // Make sure its TTL is correctly set (replaces the previous orphan detection mechanism).
                            queueRegistrationService.refreshConsumerRegistration(queueName, refreshRegistrationEvent -> {
                                if (refreshRegistrationEvent.failed()) log.warn("TODO error handling",
                                        exceptionFactory.newException("refreshRegistration(" + queueName + ") failed",
                                                refreshRegistrationEvent.cause()));
                                // And trigger its consumer.
                                notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                                    if (notifyConsumerEvent.failed()) log.warn("TODO error handling",
                                            exceptionFactory.newException("notifyConsumer(" + queueName + ") failed",
                                                    notifyConsumerEvent.cause()));
                                    if (log.isTraceEnabled()) {
                                        log.trace("refreshRegistration for queue {} time used is {} ms", queueName, System.currentTimeMillis() - perQueueStartTs);
                                    }
                                    onDone.accept(null, null);
                                });
                            });
                        };
                        ctx.redisAPI.exists(Collections.singletonList(key), event -> {
                            if (event.failed() || event.result() == null) {
                                log.error("RedisQues is unable to check existence of queue {}", queueName,
                                        exceptionFactory.newException("redisAPI.exists(" + key + ") failed", event.cause()));
                                onDone.accept(null, null);
                                return;
                            }
                            if (event.result().toLong() == 1) {
                                log.trace("Updating queue timestamp for queue '{}'", queueName);
                                // If not empty, update the queue timestamp to keep it in the sorted set.
                                queueRegistrationService.updateTimestamp(queuesKey, queueName, upTsResult -> {
                                    if (upTsResult.failed()) {
                                        log.warn("Failed to update timestamps for queue '{}'", queueName,
                                                exceptionFactory.newException("updateTimestamp(" + queueName + ") failed",
                                                        upTsResult.cause()));
                                        return;
                                    }
                                    // Ensure we clean the old queues after having updated all timestamps
                                    if (ctx.counter.decrementAndGet() == 0) {
                                        queueRegistrationService.removeOldQueues(queuesKey, ctx.limit).onComplete(removeOldQueuesEvent -> {
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
                                    queueRegistrationService.removeOldQueues(queuesKey, ctx.limit).onComplete(removeOldQueuesEvent -> {
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
                    } else {
                        onDone.accept(null, null);
                    }
                    return ctx.iter.hasNext();
                }

                @Override
                public boolean onError(Throwable ex, Void ctx_) {
                    log.warn("TODO error handling", exceptionFactory.newException(ex));
                    return true; // true, keep going with other queues.
                }

                @Override
                public void onDone(Void ctx_) {
                    // No longer used, so reduce GC graph traversal effort.
                    ctx.redisAPI = null;
                    ctx.counter = null;
                    ctx.iter = null;
                    // Mark this composition step as completed.
                    log.debug("all queue items time used is {} ms", System.currentTimeMillis() - startTs);
                    p.complete();
                }
            });
            return p.future();
        });
    }

    Future<Void> notifyConsumer(final String queueName) {
        log.debug("RedisQues Notifying consumer of queue {}", queueName);
        final EventBus eb = vertx.eventBus();
        final Promise<Void> promise = Promise.promise();
        // Find the consumer to notify
        String key = queueRegistrationService.getConsumerKey(queueName);
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
}
