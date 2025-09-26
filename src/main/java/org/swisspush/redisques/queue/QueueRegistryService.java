package org.swisspush.redisques.queue;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.QueueStatsService;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.FailedAsyncResult;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;

public class QueueRegistryService {
    private static final Logger log = LoggerFactory.getLogger(QueueRegistryService.class);
    private final RedisService redisService;
    private final RedisquesConfigurationProvider configurationProvider;
    private final MessageConsumer<String> consumersMessageConsumer;
    private final MessageConsumer<String> consumersAliveMessageConsumer;
    private final MessageConsumer<String> refreshRegistrationConsumer;
    private final MessageConsumer<String> notifyConsumer;
    private final MessageConsumer<String> uidMessageConsumer;
    private final KeyspaceHelper keyspaceHelper;
    private final Vertx vertx;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final QueueConsumerRunner queueConsumerRunner;
    private final int consumerLockTime;
    private final QueueMetrics metrics;
    private final QueueStatisticsCollector queueStatisticsCollector;
    private final UpperBoundParallel upperBoundParallel;
    private final Semaphore checkQueueRequestsQuota;
    private final Semaphore activeQueueRegRefreshReqQuota;
    private final int emptyQueueLiveTimeMillis;
    private final QueueStatsService queueStatsService;
    private Handler<Void> stoppedHandler = null;
    private PeriodicSkipScheduler periodicSkipScheduler;
    private Map<String, Long> aliveConsumers = new ConcurrentHashMap<>();


    public void stop() {
        unregisterConsumers(UnregisterConsumerType.FORCE);
    }

    public QueueRegistryService(Vertx vertx, RedisService redisService, RedisquesConfigurationProvider configurationProvider,
                                RedisQuesExceptionFactory exceptionFactory, KeyspaceHelper keyspaceHelper, QueueMetrics metrics,
                                QueueStatsService queueStatsService, QueueStatisticsCollector queueStatisticsCollector,
                                Semaphore checkQueueRequestsQuota, Semaphore activeQueueRegRefreshReqQuota) {
        this.vertx = vertx;
        this.redisService = redisService;
        this.configurationProvider = configurationProvider;
        this.keyspaceHelper = keyspaceHelper;
        this.exceptionFactory = exceptionFactory;
        this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
        this.metrics = metrics;
        this.queueStatsService = queueStatsService;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.checkQueueRequestsQuota = checkQueueRequestsQuota;
        this.activeQueueRegRefreshReqQuota = activeQueueRegRefreshReqQuota;
        String address = getConfiguration().getAddress();

        // Handles registration requests
        consumersMessageConsumer = vertx.eventBus().consumer(keyspaceHelper.getConsumersAddress(), this::handleRegistrationRequest);
        consumersAliveMessageConsumer = vertx.eventBus().consumer(keyspaceHelper.getConsumersAliveAddress(), this::handleConsumerAlive);
        refreshRegistrationConsumer = vertx.eventBus().consumer(keyspaceHelper.getVerticleRefreshRegistrationKey(), this::handleRefreshRegistration);
        notifyConsumer = vertx.eventBus().consumer(keyspaceHelper.getVerticleNotifyConsumerKey(), this::handleNotifyConsumer);

        consumerLockTime = getConfiguration().getConsumerLockMultiplier() * getConfiguration().getRefreshPeriod(); // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        queueConsumerRunner = new QueueConsumerRunner(vertx, redisService, metrics, queueStatsService, keyspaceHelper, configurationProvider, exceptionFactory, queueStatisticsCollector);
        upperBoundParallel = new UpperBoundParallel(vertx, exceptionFactory);

        // the time we let an empty queue live before we deregister ourselves
        emptyQueueLiveTimeMillis = configurationProvider.configuration().getEmptyQueueLiveTimeMillis();
        // Handles notifications
        uidMessageConsumer = vertx.eventBus().consumer(keyspaceHelper.getVerticleUid(), event -> {
            final String queue = event.body();
            if (queue == null) {
                log.warn("Got event bus msg with empty body! uid={}  address={}  replyAddress={}", keyspaceHelper.getVerticleUid(), event.address(), event.replyAddress());
                // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
            }
            log.debug("RedisQues got notification for queue '{}'", queue);
            queueConsumerRunner.consume(queue);
        });

        queueConsumerRunner.setNoMoreItemHandelr(handlder -> {
            if (stoppedHandler != null) {
                unregisterConsumers(UnregisterConsumerType.GRACEFUL).onComplete(event -> {
                    if (event.failed()) {
                        log.warn("TODO error handling", exceptionFactory.newException(
                                "unregisterConsumers() failed", event.cause()));
                    }
                    if (queueConsumerRunner.getMyQueues().isEmpty()) {
                        stoppedHandler.handle(null);
                    }
                });
            }
        });

        registerQueueCheck();
        registerKeepConsumerAlive();
        registerMyqueuesCleanup();
        registerActiveQueueRegistrationRefresh();
        registerNotExpiredQueueCheck();
        this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
    }

    public QueueConsumerRunner getQueueConsumerRunner() {
        return queueConsumerRunner;
    }

    public Future<Void> unregisterAll(Collection<MessageConsumer<?>> consumers) {
        List<Future<?>> futures = consumers.stream()
                .filter(Objects::nonNull)
                .filter(MessageConsumer::isRegistered)
                .map(MessageConsumer::unregister)
                .collect(Collectors.toList());

        if (futures.isEmpty()) return Future.succeededFuture();
        return Future.join(futures).mapEmpty();
    }

    private RedisquesConfiguration getConfiguration() {
        return configurationProvider.configuration();
    }

    private void handleRefreshRegistration(Message<String> msg) {
        final String queueName = msg.body();
        refreshRegistration(queueName, event -> {
            if (event.succeeded()) {
                msg.reply(null);
            } else {
                msg.fail(0, event.cause().getMessage());
            }
        });
    }

    private void handleNotifyConsumer(Message<String> msg) {
        final String queueName = msg.body();
        notifyConsumer(queueName).onComplete(event -> {
            if (event.succeeded()) {
                msg.reply(null);
            } else {
                msg.fail(0, event.cause().getMessage());
            }
        });
    }

    private void handleConsumerAlive(Message<String> msg) {
        final String consumerId = msg.body();
        final long periodMs = getConfiguration().getRefreshPeriod() * 1000L;
        aliveConsumers.put(consumerId, currentTimeMillis() + (periodMs * 4));
        log.debug("RedisQues consumer {} keep alive renewed", consumerId);
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
        log.debug("RedisQues Got registration request for queue {} from consumer: {}", queueName, keyspaceHelper.getVerticleUid());
        // Try to register for this queue
        tryRegister(keyspaceHelper.getConsumersPrefix() + queueName, keyspaceHelper.getVerticleUid()).onComplete(new Handler<AsyncResult<Boolean>>() {
            @Override
            public void handle(AsyncResult<Boolean> event) {
                if (event.succeeded()) {
                    metrics.perQueueMetricsReg(queueName);
                    boolean setDone = event.result() != null ? event.result() : false;
                    log.trace("RedisQues setxn result: {} for queue: {}", setDone, queueName);
                    metrics.consumerCounterIncrement(1);
                    if (setDone) {
                        // I am now the registered consumer for this queue.
                        log.debug("RedisQues Now registered for queue {}", queueName);
                        queueConsumerRunner.setMyQueuesState(queueName, QueueState.READY);
                        queueConsumerRunner.consume(queueName);
                    } else {
                        log.debug("RedisQues Missed registration for queue {}", queueName);
                        // Someone else just became the registered consumer. I
                        // give up.
                    }
                } else {
                    log.error("redisSetWithOptions failed", event.cause());
                }
            }
        });
    }

    // QueueRegistry
    public Future<Boolean> tryRegister(String queueName, String uid) {
        return redisService.setNxPx(queueName, uid, true, 1000L * consumerLockTime);
    }

    public void refreshRegistration(String queueName, Handler<AsyncResult<Response>> handler) {
        log.debug("RedisQues Refreshing registration of queue consumer {}, expire in {} s", queueName, consumerLockTime);
        String consumerKey = keyspaceHelper.getConsumersPrefix() + queueName;
        if (handler == null) {
            throw new RuntimeException("Handler must be set");
        } else {
            redisService.expire(consumerKey, String.valueOf(consumerLockTime)).onComplete(handler);
        }
    }

    private void registerActiveQueueRegistrationRefresh() {
        // Periodic refresh of my registrations on active queues.
        var periodMs = getConfiguration().getRefreshPeriod() * 1000L;
        periodicSkipScheduler.setPeriodic(periodMs, "registerActiveQueueRegistrationRefresh", new Consumer<Runnable>() {
            Iterator<Map.Entry<String, QueueProcessingState>> iter;

            @Override
            public void accept(Runnable onPeriodicDone) {
                // Need a copy to prevent concurrent modification issuses.
                iter = new HashMap<>(queueConsumerRunner.getMyQueues()).entrySet().iterator();
                // Trigger only a limited amount of requests in parallel.
                upperBoundParallel.request(activeQueueRegRefreshReqQuota, iter, new UpperBoundParallel.Mentor<>() {
                    @Override
                    public boolean runOneMore(BiConsumer<Throwable, Void> onQueueDone, Iterator<Map.Entry<String, QueueProcessingState>> iter) {
                        refreshConsumerRegistration(onQueueDone);
                        return iter.hasNext();
                    }

                    @Override
                    public boolean onError(Throwable ex, Iterator<Map.Entry<String, QueueProcessingState>> iter) {
                        if (log.isWarnEnabled()) log.warn("TODO error handling", exceptionFactory.newException(ex));
                        onPeriodicDone.run();
                        return false;
                    }

                    @Override
                    public void onDone(Iterator<Map.Entry<String, QueueProcessingState>> iter) {
                        onPeriodicDone.run();
                    }
                });
            }

            void refreshConsumerRegistration(BiConsumer<Throwable, Void> onQueueDone) {
                while (iter.hasNext()) {
                    var entry = iter.next();
                    if (entry.getValue().getState() != QueueState.CONSUMING) continue;
                    checkIfImStillTheRegisteredConsumer(entry.getKey(), onQueueDone);
                    return;
                }
                // no entry found. we're done.
                onQueueDone.accept(null, null);
            }

            void checkIfImStillTheRegisteredConsumer(String queue, BiConsumer<Throwable, Void> onDone) {
                // Check if I am still the registered consumer
                String consumerKey = keyspaceHelper.getConsumersPrefix() + queue;
                log.trace("RedisQues refresh queues get: {}", consumerKey);
                redisService.get(consumerKey).onComplete(getConsumerEvent -> {
                    if (getConsumerEvent.failed()) {
                        Throwable ex = exceptionFactory.newException(
                                "Failed to get queue consumer for queue '" + queue + "'", getConsumerEvent.cause());
                        assert ex != null;
                        onDone.accept(ex, null);
                        return;
                    }
                    final String consumer = Objects.toString(getConsumerEvent.result(), "");
                    if (keyspaceHelper.getVerticleUid().equals(consumer)) {
                        log.debug("RedisQues Periodic consumer refresh for active queue {}", queue);
                        refreshRegistration(queue, ev -> {
                            if (ev.failed()) {
                                onDone.accept(exceptionFactory.newException("TODO error handling", ev.cause()), null);
                                return;
                            }
                            metrics.perQueueMetricsRefresh(queue);
                            updateTimestamp(queue, ev3 -> {
                                Throwable ex = ev3.succeeded() ? null : exceptionFactory.newException(
                                        "updateTimestamp(" + queue + ") failed", ev3.cause());
                                onDone.accept(ex, null);
                            });
                        });
                    } else {
                        log.debug("RedisQues Removing queue {} from the list", queue);
                        queueConsumerRunner.getMyQueues().remove(queue);
                        queueStatisticsCollector.resetQueueFailureStatistics(queue, onDone);
                    }
                });
            }
        });
    }

    private void registerKeepConsumerAlive() {
        final long periodMs = getConfiguration().getRefreshPeriod() * 1000L;
        vertx.setPeriodic(10, periodMs, event -> {
            Iterator<Map.Entry<String, Long>> iterator = aliveConsumers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                if (currentTimeMillis() > entry.getValue()) {
                    log.info("RedisQues consumer with id {}' has expired", entry.getKey());
                    iterator.remove();
                    metrics.consumerCounterIncrement(-1);
                }
            }
            vertx.eventBus().publish(keyspaceHelper.getConsumersAliveAddress(), keyspaceHelper.getVerticleUid());
            log.debug("RedisQues consumer {} keep alive published", keyspaceHelper.getVerticleUid());
        });
    }

    private Future<Void> unregisterConsumers(UnregisterConsumerType type) {
        final Promise<Void> result = Promise.promise();
        log.debug("RedisQues unregister consumers. type={}", type);
        final List<Future> futureList = new ArrayList<>(queueConsumerRunner.getMyQueues().size());
        for (final Map.Entry<String, QueueProcessingState> entry : queueConsumerRunner.getMyQueues().entrySet()) {
            final String queueName = entry.getKey();
            final QueueProcessingState state = entry.getValue();
            switch (type) {
                case FORCE:
                    futureList.add(unregisterQueue(queueName));
                    break;
                case GRACEFUL:
                    if (entry.getValue().getState() == QueueState.READY) {
                        futureList.add(unregisterQueue(queueName));
                    }
                    break;
                case QUIET_FOR_SOMETIME:
                    if (emptyQueueLiveTimeMillis <= 0) {
                        break; // disabled
                    }
                    if (state.getLastConsumedTimestampMillis() > 0
                            && System.currentTimeMillis() > state.getLastConsumedTimestampMillis() + emptyQueueLiveTimeMillis) {
                        // the queue has been empty for quite a while now
                        log.debug("empty queue {} has has been idle for {} ms, deregister", queueName, emptyQueueLiveTimeMillis);
                        futureList.add(unregisterQueue(queueName));
                    } else {
                        log.debug("queue {} is empty since: {}", queueName, state.getLastConsumedTimestampMillis());
                    }
                    break;
                default:
                    log.error("Unsupported UnregisterConsumerType: {}", type);
            }
        }
        CompositeFuture.all(futureList).onComplete(ev -> {
            if (ev.failed()) log.warn("TODO error handling", exceptionFactory.newException(ev.cause()));
            result.complete();
        });
        return result.future();
    }

    private Future<Void> unregisterQueue(String queueName) {
        log.trace("RedisQues unregister consumers queue: {}", queueName);
        Promise<Void> promise = Promise.promise();
        refreshRegistration(queueName, event -> {
            if (event.failed()) {
                log.warn("TODO error handling", exceptionFactory.newException(
                        "refreshRegistration(" + queueName + ") failed", event.cause()));
            }
            metrics.perQueueMetricsRefresh(queueName);
            // Make sure that I am still the registered consumer
            final String consumerKey = keyspaceHelper.getConsumersPrefix() + queueName;
            log.trace("RedisQues unregister consumers get: {}", consumerKey);
            redisService.get(consumerKey).onComplete(getEvent -> {
                if (getEvent.failed()) {
                    log.warn("Failed to retrieve consumer '{}'.", consumerKey, getEvent.cause());
                    // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                }
                String consumer = Objects.toString(getEvent.result(), "");
                log.trace("RedisQues unregister consumers get result: {}", consumer);
                if (keyspaceHelper.getVerticleUid().equals(consumer)) {
                    log.debug("RedisQues remove consumer: {}", keyspaceHelper.getVerticleUid());
                    queueConsumerRunner.getMyQueues().remove(queueName);
                    redisService.del(Collections.singletonList(consumerKey)).onComplete(delResult -> {
                        if (delResult.failed()) {
                            log.warn("Failed to deregister myself from queue '{}'", consumerKey, exceptionFactory.newException(delResult.cause()));
                        } else {
                            metrics.perQueueMetricsRemove(queueName);
                            log.debug("Deregistered myself from queue '{}'", consumerKey);
                        }
                        promise.complete();
                    });
                } else {
                    promise.complete();
                }
            });
        });
        return promise.future();
    }

    /**
     * Caution: this may in some corner case violate the ordering for one
     * message.
     */
    public void resetConsumers() {
        log.debug("RedisQues Resetting consumers");
        String keysPattern = keyspaceHelper.getConsumersPrefix() + "*";
        log.trace("RedisQues reset consumers keys: {}", keysPattern);
        redisService.keys(keysPattern).onComplete(keysResult -> {
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
            redisService.del(args).onComplete(delManyResult -> {
                if (delManyResult.succeeded()) {
                    if (log.isDebugEnabled())
                        log.debug("Successfully reset {} consumers", delManyResult.result().toLong());
                } else {
                    log.error("Unable to delete redis keys of consumers");
                }
            });
        });
    }

    private void registerQueueCheck() {
        periodicSkipScheduler.setPeriodic(configurationProvider.configuration().getCheckIntervalTimerMs(), "checkQueues", onDone -> {
            int checkInterval = configurationProvider.configuration().getCheckInterval();
            redisService.setNxPx(keyspaceHelper.getQueueCheckLastExecKey(), String.valueOf(currentTimeMillis()), true, 1000L * checkInterval).compose(aBoolean -> {
                log.info("periodic queue check is triggered now");
                return checkQueues();
            }).onComplete((AsyncResult<Void> ev) -> {
                if (ev.failed()) {
                    if (log.isErrorEnabled())
                        log.error("TODO error handling", exceptionFactory.newException(ev.cause()));
                }
                onDone.run();
            });
        });
    }

    /**
     * Notify not-active/not-empty queues to be processed (e.g. after a reboot).
     * Check timestamps of not-active/empty queues.
     * This uses a sorted set of queue names scored by last update timestamp.
     */
    public Future<Void> checkQueues() {
        final long startTs = System.currentTimeMillis();
        final var ctx = new Object() {
            long limit;
            AtomicInteger counter;
            Iterator<Response> iter;
        };

        return Future.<Void>succeededFuture().compose((Void v) -> {
            log.debug("Checking queues timestamps");
            // List all queues that look inactive (i.e. that have not been updated since 3 periods).
            ctx.limit = currentTimeMillis() - 3L * configurationProvider.configuration().getRefreshPeriod() * 1000;
            return redisService.zrangebyscore(keyspaceHelper.getQueuesKey(), "-inf", String.valueOf(ctx.limit));
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
                        String key = keyspaceHelper.getQueuesPrefix() + queueName;
                        log.trace("RedisQues update queue: {}", key);
                        Handler<Void> refreshRegHandler = event -> {
                            // Make sure its TTL is correctly set (replaces the previous orphan detection mechanism).
                            refreshRegistration(queueName, refreshRegistrationEvent -> {
                                if (refreshRegistrationEvent.failed()) log.warn("TODO error handling",
                                        exceptionFactory.newException("refreshRegistration(" + queueName + ") failed",
                                                refreshRegistrationEvent.cause()));
                                metrics.perQueueMetricsRefresh(queueName);
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
                        redisService.exists(Collections.singletonList(key)).onComplete(event -> {
                            if (event.failed() || event.result() == null) {
                                log.error("RedisQues is unable to check existence of queue {}", queueName,
                                        exceptionFactory.newException("redisAPI.exists(" + key + ") failed", event.cause()));
                                onDone.accept(null, null);
                                return;
                            }
                            if (event.result().toLong() == 1) {
                                log.trace("Updating queue timestamp for queue '{}'", queueName);
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
                                queueStatsService.dequeueStatisticMarkedForRemoval(queueName);
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

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queueName name of the queue
     * @param handler   (optional) To get informed when done.
     */
    private void updateTimestamp(final String queueName, Handler<AsyncResult<Response>> handler) {
        long ts = System.currentTimeMillis();
        log.trace("RedisQues update timestamp for queue: {} to: {}", queueName, ts);
        redisService.zadd(keyspaceHelper.getQueuesKey(), queueName, String.valueOf(ts)).onSuccess(event -> {
            if (handler != null) {
                handler.handle(Future.succeededFuture(event));
            }
        }).onFailure(throwable -> {
            log.warn("Redis: Error in updateTimestamp", throwable);
            if (handler != null) {
                handler.handle(new FailedAsyncResult<>(throwable));
            }
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
        redisService.zremrangebyscore(keyspaceHelper.getQueuesKey(), "-inf", String.valueOf(limit)).onComplete(event -> {
            if (event.failed() && log.isWarnEnabled()) log.warn("TODO error handling",
                    exceptionFactory.newException("redisAPI.zremrangebyscore('" + keyspaceHelper.getQueuesKey() + "', '-inf', " + limit + ") failed",
                            event.cause()));
            promise.complete();
        });
        return promise.future();
    }

    private void registerMyqueuesCleanup() {
        if (emptyQueueLiveTimeMillis <= 0) {
            return; // disabled
        }
        final long periodMs = configurationProvider.configuration().getRefreshPeriod() * 1000L;
        vertx.setPeriodic(10000, periodMs, event -> {
            unregisterConsumers(UnregisterConsumerType.QUIET_FOR_SOMETIME);
        });
    }

    public void gracefulStop(final Handler<Void> doneHandler) {
        unregisterAll(Arrays.asList(consumersMessageConsumer, uidMessageConsumer,
                consumersAliveMessageConsumer, refreshRegistrationConsumer, notifyConsumer)).onComplete(event -> {
            if (event.failed()) {
                log.warn("TODO error handling", exceptionFactory.newException(
                        "unregisterConsumers() failed", event.cause()));
            }
            unregisterConsumers(UnregisterConsumerType.GRACEFUL).onComplete(unregisterConsumersEvent -> {
                if (unregisterConsumersEvent.failed()) {
                    log.warn("TODO error handling", exceptionFactory.newException(
                            "unregisterConsumers(false) failed", unregisterConsumersEvent.cause()));
                }

                queueConsumerRunner.trimRequestConsumerUnregister(unregisterTrimEvent -> {
                    if (unregisterTrimEvent.failed()) {
                        log.warn("TODO error handling", exceptionFactory.newException(
                                "unregister trimRequestConsumer failed", unregisterTrimEvent.cause()));
                    }
                    stoppedHandler = doneHandler;
                    if (queueConsumerRunner.getMyQueues().keySet().isEmpty()) {
                        doneHandler.handle(null);
                    }
                });
            });
        });

    }

    private Future<Void> notifyConsumer(final String queueName) {
        log.debug("RedisQues Notifying consumer of queue {}", queueName);
        final EventBus eb = vertx.eventBus();
        final Promise<Void> promise = Promise.promise();
        // Find the consumer to notify
        String key = keyspaceHelper.getConsumersPrefix() + queueName;
        log.trace("RedisQues notify consumer get: {}", key);
        redisService.get(key).onComplete(event -> {
            if (event.failed()) {
                log.warn("Failed to get consumer for queue '{}'", queueName, new Exception(event.cause()));
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            String consumer = Objects.toString(event.result(), null);
            log.trace("RedisQues got consumer: {}", consumer);
            if (consumer == null) {
                // No consumer for this queue, let's make a peer become consumer
                log.debug("RedisQues Sending registration request for queue {}", queueName);
                eb.send(keyspaceHelper.getConsumersAddress(), queueName);
                promise.complete();
            } else if (!aliveConsumers.containsKey(consumer)) {
                log.warn("RedisQues consumer {} of queue {} does not exist.", consumer, queueName);
                redisService.del(Collections.singletonList(key)).onComplete(result -> {
                    if (result.failed()) {
                        log.warn("Failed to removed consumer '{}'", key, exceptionFactory.newException(result.cause()));
                    } else {
                        log.debug("{} consumer key removed", result.result().toLong());
                        // need find a new consumer for this queue, let's make a peer become consumer
                        log.debug("RedisQues Sending new registration request for queue {}", queueName);
                        eb.send(keyspaceHelper.getConsumersAddress(), queueName);
                    }
                    promise.complete();
                });
            } else {
                // Notify the registered consumer
                log.debug("RedisQues Notifying consumer {} to consume queue {}", consumer, queueName);
                eb.send(consumer, queueName);
                promise.complete();
            }
        });
        return promise.future();
    }

    private void registerNotExpiredQueueCheck() {
        vertx.setPeriodic(20 * 1000, event -> {
            if (!log.isDebugEnabled()) {
                return;
            }
            String keysPattern = keyspaceHelper.getConsumersPrefix() + "*";
            log.debug("RedisQues list not expired consumers keys:");
            redisService.scan("0", keysPattern, "1000", null).onComplete(keysResult -> {
                if (keysResult.failed() || keysResult.result() == null || keysResult.result().size() != 2) {
                    log.error("Unable to get redis keys of consumers", keysResult.cause());
                    return;
                }
                Response keys = keysResult.result().get(1);
                if (keys == null || keys.size() == 0) {
                    log.debug("0 not expired consumers keys found");
                    return;
                }

                if (log.isTraceEnabled()) {
                    for (Response response : keys) {
                        log.trace(response.toString());
                    }
                }
                log.debug("{} not expired consumers keys found, {} keys in myQueues list", keys.size(), queueConsumerRunner.getMyQueues().size());
            });
        });
    }
}
