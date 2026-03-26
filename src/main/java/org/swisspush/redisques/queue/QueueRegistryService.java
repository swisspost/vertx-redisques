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
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.QueueStatsService;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;

public class QueueRegistryService {
    private static final Logger log = LoggerFactory.getLogger(QueueRegistryService.class);
    private final RedisService redisService;
    private final RedisquesConfigurationProvider configurationProvider;
    private final MessageConsumer<String> consumersMessageConsumer;
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
    protected Set<String> aliveConsumers = ConcurrentHashMap.newKeySet();


    public void stop() {
        unregisterConsumers(UnregisterConsumerType.FORCE);
    }

    public QueueRegistryService(Vertx vertx, RedisService redisService, RedisquesConfigurationProvider configurationProvider,
                                RedisQuesExceptionFactory exceptionFactory, KeyspaceHelper keyspaceHelper, QueueMetrics metrics,
                                QueueStatsService queueStatsService, QueueStatisticsCollector queueStatisticsCollector,
                                Semaphore checkQueueRequestsQuota, Semaphore activeQueueRegRefreshReqQuota,
                                QueueConfigurationProvider queueConfigurationProvider) {
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

        // Handles registration requests
        consumersMessageConsumer = vertx.eventBus().consumer(keyspaceHelper.getConsumersAddress(), this::handleRegistrationRequest);
        notifyConsumer = vertx.eventBus().consumer(keyspaceHelper.getVerticleNotifyConsumerKey(), this::handleNotifyConsumer);

        consumerLockTime = getConfiguration().getConsumerLockMultiplier() * getConfiguration().getRefreshPeriod(); // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        queueConsumerRunner = new QueueConsumerRunner(vertx, redisService, metrics, queueStatsService, keyspaceHelper, configurationProvider, exceptionFactory, queueStatisticsCollector, queueConfigurationProvider);
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

        // register my self into alive consumer first.
        registerKeepConsumerAlive();
        registerQueueCheck();
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

    private void registerKeepConsumerAlive() {
        // initial set, add self into local list first
        aliveConsumers.add(keyspaceHelper.getVerticleUid());

        // keep key alive 2 times of refresh period
        final long keyLiveTime = getConfiguration().getRefreshPeriod() * 1000L * 2;

        // add self into Redis with expiring time, without wait.
        updateConsumerIdAndRemoveExpired(keyspaceHelper.getVerticleUid(), keyLiveTime)
                .onFailure(e -> log.warn("failed to set initial alive consumer live key for {}", keyspaceHelper.getVerticleUid(), e));

        // update 2 heartbeat timestamp per refresh period
        final long periodMs = Math.max(getConfiguration().getRefreshPeriod() / 2 * 1000L, 1);

        vertx.setPeriodic(periodMs, event -> {
            updateConsumerIdAndRemoveExpired(keyspaceHelper.getVerticleUid(), keyLiveTime).onComplete(event2 -> {
                if (event2.failed()) {
                    log.warn("failed to update alive consumer live key for {}", keyspaceHelper.getVerticleUid(), event2.cause());
                } else {
                    log.debug("RedisQues consumer {} keep alive updated", keyspaceHelper.getVerticleUid());
                }
                getAllValidConsumerIds(keyLiveTime).onComplete(event1 -> {
                    if (event1.failed()) {
                        log.warn("failed to get alive consumer list", event1.cause());
                        return;
                    }
                    List<String> newlist = event1.result();
                    // add all first
                    aliveConsumers.addAll(newlist);
                    // remove older which not in new list
                    aliveConsumers.retainAll(newlist);
                    // ensure self is inside
                    aliveConsumers.add(keyspaceHelper.getVerticleUid());
                });
            });
        });
    }

    /**
     * get all not expired consumer Ids
     * @param expireMs all values older than X ms will be removed
     * @return
     */
    Future<List<String>> getAllValidConsumerIds(long expireMs) {
        Promise<List<String>> promise = Promise.promise();

        final long expireScore = System.currentTimeMillis() - expireMs;

        List<Request> requests = new ArrayList<>();

        // Remove expired
        requests.add(Request.cmd(Command.ZREMRANGEBYSCORE)
                .arg(keyspaceHelper.getAliveConsumersKey())
                .arg("0")
                .arg(String.valueOf(expireScore)));

        // Get all remaining values
        requests.add(Request.cmd(Command.ZRANGE)
                .arg(keyspaceHelper.getAliveConsumersKey())
                .arg("0")
                .arg("-1"));

        redisService.batch(requests).onComplete(res -> {
            if (res.failed()) {
                log.error("failed to fetch alive consumer list", res.cause());
                promise.fail(res.cause());
                return;
            }

            Response zrangeResponse = res.result().get(1);
            List<String> values = new ArrayList<>();

            for (int i = 0; i < zrangeResponse.size(); i++) {
                values.add(zrangeResponse.get(i).toString());
            }
            promise.complete(values);
        });

        return promise.future();
    }

    /**
     * Update or added a consumer Id with current time, and remove expired
     * @param consumerId consumer id needs to be added or update
     * @param expireMs all values older than X ms will be removed
     * @return
     */
    Future<Void> updateConsumerIdAndRemoveExpired(String consumerId, long expireMs) {
        Promise<Void> promise = Promise.promise();

        final long now = System.currentTimeMillis();
        final long expireScore = now - expireMs;

        List<Request> requests = new ArrayList<>();

        // Add or update consumer id
        requests.add(Request.cmd(Command.ZADD)
                .arg(keyspaceHelper.getAliveConsumersKey())
                .arg(String.valueOf(now))
                .arg(consumerId));

        // Remove expired
        requests.add(Request.cmd(Command.ZREMRANGEBYSCORE)
                .arg(keyspaceHelper.getAliveConsumersKey())
                .arg("0")
                .arg(String.valueOf(expireScore)));

        redisService.batch(requests).onComplete(res -> {
            if (res.failed()) {
                log.warn("failed to add or update live consumer id{}", consumerId, res.cause());
                promise.fail(res.cause());
            } else {
                promise.complete();
            }
        });

        return promise.future();
    }

    /**
     * <p>Handler receiving registration requests when no consumer is registered
     * for a queue.</p>
     */
    void handleRegistrationRequest(Message<String> msg) {
        final String queueName = msg.body();
        if (queueName == null) {
            log.warn("Got message without queue name while handleRegistrationRequest.");
            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
        }

        registerQueue(queueName);
    }

    void registerQueue(String queueName) {
        log.debug("RedisQues Got registration request for queue {} from consumer: {}", queueName, keyspaceHelper.getVerticleUid());
        // Try to register for this queue
        tryRegister(keyspaceHelper.getConsumersPrefix() + queueName, keyspaceHelper.getVerticleUid()).onComplete(new Handler<AsyncResult<Boolean>>() {
            @Override
            public void handle(AsyncResult<Boolean> event) {
                if (event.succeeded()) {
                    metrics.perQueueMetricsReg(queueName);
                    boolean setDone = event.result() != null ? event.result() : false;
                    log.trace("RedisQues setxn result: {} for queue: {}", setDone, queueName);
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
            vertx.executeBlocking(() -> redisService.expire(consumerKey, String.valueOf(consumerLockTime)))
                    .compose((Future<Response> tooManyNestedFutures) -> tooManyNestedFutures)
                    .onComplete(event -> {
                        getQueueConsumerRunner().updateLastRefreshRegistrationTimeStamp(queueName);
                        handler.handle(event);
                    });
        }
    }

    /**
     * refresh a set of queue registration
     * @param queueNames
     * @return a list of response of each refresh
     */
    public Future<List<Response>> batchRefreshRegistration(List<String> queueNames) {
        if (queueNames == null || queueNames.isEmpty()) {
            log.debug("Got empty queue names for refresh registration");
            return Future.succeededFuture(new ArrayList<>());
        }
        Promise<List<Response>> promise = Promise.promise();

        log.debug("RedisQues Refreshing registration of {} queue consumers, expire in {} s",
                queueNames.size(), consumerLockTime);
        final String consumersPrefix = keyspaceHelper.getConsumersPrefix();
        List<Request> requests = new ArrayList<>(queueNames.size());
        for (String queueName : queueNames) {
            String consumerKey = consumersPrefix + queueName;
            requests.add(Request.cmd(Command.EXPIRE)
                    .arg(consumerKey)
                    .arg(String.valueOf(consumerLockTime)));
            metrics.perQueueMetricsRefresh(queueName);
        }

        redisService.batch(requests).onComplete(event -> {
            if (event.failed()) {
                log.error("batchRefreshRegistration failed with message: {}", event.cause().getMessage());
                promise.fail(event.cause());
            } else {
                for (String queueName : queueNames) {
                    getQueueConsumerRunner().updateLastRefreshRegistrationTimeStamp(queueName);
                }
                promise.complete(event.result());
            }
        });
        return promise.future();
    }

    /**
     * reorder the myqueues list, sorted by old to new by LastRegisterRefreshedMillis
     *
     * @return
     */
    Map<String, QueueProcessingState> getSortedMyQueueClone(Map<String, QueueProcessingState> myQueues) {
        return
                myQueues.entrySet()
                        .stream()
                        .sorted(Comparator.comparingLong(e -> e.getValue().getLastRegisterRefreshedMillis()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (a, b) -> a,
                                LinkedHashMap::new
                        ));
    }

    /**
     * split a Map into a list of map chunk, depends on chunk size
     * @param map source
     * @param chunkSize
     * @return a linked list
     */
    <K, V> List<Map<K, V>> splitMap(Map<K, V> map, int chunkSize) {
        final List<Map<K, V>> result = new ArrayList<>();
        Map<K, V> current = new LinkedHashMap<>();
        int itemCount = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            current.put(entry.getKey(), entry.getValue());
            itemCount++;

            if (itemCount >= chunkSize) {
                result.add(current);
                current = new LinkedHashMap<>();
                itemCount = 0;
            }
        }
        if (!current.isEmpty()) {
            result.add(current);
        }
        return result;
    }

    private void registerActiveQueueRegistrationRefresh() {
        // Periodic refresh of my registrations on active queues.
        final long periodMs = getConfiguration().getRefreshPeriod() * 1000L;
        final int activeQueueRegRefreshReqQuotaAcquireRetryTime = getConfiguration().getActiveQueueRegRefreshReqQuotaAcquireRetryTimeMs();
        periodicSkipScheduler.setPeriodic(periodMs, "registerActiveQueueRegistrationRefresh", new Consumer<Runnable>() {
            Iterator<Map<String, QueueProcessingState>> iter;

            @Override
            public void accept(Runnable onPeriodicDone) {

                // Need a copy to prevent concurrent modification issuses.
                List<Map<String, QueueProcessingState>> myQueueSplitClone = splitMap(getSortedMyQueueClone(queueConsumerRunner.getMyQueues()), RedisService.MAX_COMMANDS_IN_BATCH);
                iter = myQueueSplitClone.iterator();
                // Trigger only a limited amount of requests in parallel.
                upperBoundParallel.request(activeQueueRegRefreshReqQuota, activeQueueRegRefreshReqQuotaAcquireRetryTime, iter, new UpperBoundParallel.Mentor<>() {
                    @Override
                    public boolean runOneMore(BiConsumer<Throwable, Void> onQueueDone, Iterator<Map<String, QueueProcessingState>> iter) {
                        batchRefreshConsumerRegistration(onQueueDone);
                        return iter.hasNext();
                    }

                    @Override
                    public boolean onError(Throwable ex, Iterator<Map<String, QueueProcessingState>> iter) {
                        if (log.isWarnEnabled()) {
                            log.warn("TODO error handling", exceptionFactory.newException(ex));
                        }
                        // just continue to refresh next one
                        return true;
                    }

                    @Override
                    public void onDone(Iterator<Map<String, QueueProcessingState>> iter) {
                        onPeriodicDone.run();
                    }
                });
            }

            void batchRefreshConsumerRegistration(BiConsumer<Throwable, Void> onQueueDone) {
                if (iter.hasNext()) {
                    Map<String, QueueProcessingState> mapPerSet = iter.next();
                    List<String> queueNames = new ArrayList<>(RedisService.MAX_COMMANDS_IN_BATCH);
                    for (Map.Entry<String, QueueProcessingState> entry : mapPerSet.entrySet()) {
                        QueueProcessingState state = entry.getValue();
                        if (state.getState() != QueueState.CONSUMING) {
                            log.trace("nothing to be done for this entry because state is: {}", state);
                            continue;
                        }
                        queueNames.add(entry.getKey());
                    }
                    if (queueNames.isEmpty()) {
                        log.trace("nothing to be done for this entry");
                        onQueueDone.accept(null, null);
                        return;
                    }
                    batchCheckIfImStillTheRegisteredConsumer(queueNames, keyspaceHelper.getVerticleUid()).onComplete(event -> {
                        if (event.failed()) {
                            log.error("Failed to get queue consumer for queues", event.cause());
                            onQueueDone.accept(event.cause(), null);
                        }else {
                            log.debug("RedisQues Periodic consumer refresh for active queues");
                            final List<String> myQueueNames = event.result().entrySet()
                                    .stream()
                                    .filter(e -> Boolean.TRUE.equals(e.getValue()))
                                    .map(Map.Entry::getKey)
                                    .collect(Collectors.toList());

                            final List<String> notMyQueueNames = event.result().entrySet()
                                    .stream()
                                    .filter(e -> Boolean.FALSE.equals(e.getValue()))
                                    .map(Map.Entry::getKey)
                                    .collect(Collectors.toList());

                            batchRefreshRegistration(myQueueNames).onComplete(event1 -> {
                                if (event1.failed()) {
                                    log.warn("failed to batch update queues registration", event1.cause());
                                    onQueueDone.accept(event1.cause(), null);
                                    return;
                                }

                                int refreshRegistrationErrorCounter = countErrorsAndNulls(event1.result());
                                if (refreshRegistrationErrorCounter > 0) {
                                    log.warn("() queue failed to update queues registration", event1.cause());
                                }
                                updateTimestampsWithOverwrite(myQueueNames).onComplete(event2 -> {
                                    if (event2.failed()) {
                                        log.warn("failed to batch update queues timestamp", event2.cause());
                                        onQueueDone.accept(event2.cause(), null);
                                        return;
                                    }
                                    if (notMyQueueNames.isEmpty()) {
                                        onQueueDone.accept(null, null);
                                    } else {
                                        List<Future<?>> futures = new ArrayList<>(notMyQueueNames.size());
                                        notMyQueueNames.forEach(queue -> {
                                            log.debug("RedisQues Removing queue {} from the list", queue);
                                            queueConsumerRunner.getMyQueues().remove(queue);
                                            // This queue is not owned by this instance; removing it from the local dequeue statistics cache.
                                            queueStatsService.dequeueStatisticRemoveFromLocal(queue);
                                            futures.add(queueStatisticsCollector.resetQueueFailureStatistics(queue));
                                        });
                                        Future.all(futures).onComplete(event3 -> {
                                            if (event3.failed()) {
                                                log.warn("failed to remove queue stats not owen by this consumer", event3.cause());
                                            }
                                            onQueueDone.accept(null, null);
                                        });
                                    }
                                });
                            });
                        }
                    });
                }
            }
        });
    }

    /**
     * check do give queue's consumer match given one.
     * @param queueNames queue names need to check
     * @param consumerId consumer id will match
     * @return a Future continues a map, Key is queue name, Value ture if matches, false not match, NULL queue not exist.
     */
    Future<Map<String, Boolean>> batchCheckIfImStillTheRegisteredConsumer(List<String> queueNames, String consumerId) {
        List<String> queueConsumersKeys = queueNames.stream()
                .map(queue -> keyspaceHelper.getConsumersPrefix() + queue)
                .collect(Collectors.toList());

        return redisService.get(queueConsumersKeys).compose(response -> {
            Map<String, Boolean> responses = new HashMap<>();

            for (int i = 0; i < queueConsumersKeys.size(); i++) {
                Response res = response.get(i);
                // Check if I am still the registered consumer
                responses.put(queueNames.get(i), res == null ? null : consumerId.equals(res.toString()));
            }
            return Future.succeededFuture(responses);
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
                    // This queue is not owned by this instance; removing it from the local dequeue statistics cache.
                    queueStatsService.dequeueStatisticRemoveFromLocal(queueName);
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
        // List all queues that look inactive (i.e. that have not been updated since 3 periods).
        final long limit = currentTimeMillis() - 3L * configurationProvider.configuration().getRefreshPeriod() * 1000L;
        final int batchSize = RedisService.MAX_COMMANDS_IN_BATCH;
        final int checkQueueRequestsQuotaAcquireRetryTime = configurationProvider.configuration().getCheckQueueRequestsQuotaAcquireRetryTimeMs();
        log.debug("Checking queues timestamps");
        return redisService.zrangebyscore(keyspaceHelper.getQueuesKey(), "-inf", String.valueOf(limit))
                .onFailure(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable event) {
                        log.error("Unable to check queues timestamps by zrangebyscore", event);
                    }
                })
                .compose(queues -> {
                    if (log.isDebugEnabled()) {
                        log.debug("zrangebyscore time used is {} ms", System.currentTimeMillis() - startTs);
                    }
                    List<List<String>> processQueue = new ArrayList<>();
                    List<String> currentBatch = new ArrayList<>(batchSize);

                    // create a batch with MAX_COMMANDS_IN_BATCH
                    for (Response queue : queues) {
                        currentBatch.add(queue.toString());
                        if (currentBatch.size() == batchSize) {
                            processQueue.add(currentBatch);
                            currentBatch = new ArrayList<>(batchSize);
                        }
                    }

                    if (!currentBatch.isEmpty()) {
                        // if batch is not empty, add it to process queue
                        processQueue.add(currentBatch);
                    }
                    Promise<Void> promise = Promise.promise();
                    Iterator<List<String>> iter = processQueue.iterator();
                    // limits on process queue size
                    upperBoundParallel.request(checkQueueRequestsQuota, checkQueueRequestsQuotaAcquireRetryTime, iter, new UpperBoundParallel.Mentor<>() {
                        @Override
                        public boolean runOneMore(BiConsumer<Throwable, Void> onDone, Iterator<List<String>> iterator) {
                            if (!iterator.hasNext()) {
                                onDone.accept(null, null);
                                return false;
                            }

                            List<String> batch = iterator.next();
                            batchCheckQueuesSize(batch).onComplete(asyncResult -> {
                                if (asyncResult.failed()) {
                                    log.warn("failed to batch check queue exist", asyncResult.cause());
                                    onDone.accept(asyncResult.cause(), null);
                                } else {
                                    Map<String, Integer> result = asyncResult.result();
                                    List<String> nonEmptyQueue = new ArrayList<>();
                                    List<String> emptyQueue = new ArrayList<>();
                                    List<String> failedQueue = new ArrayList<>();
                                    result.forEach((key, value) -> {
                                        if (value == null) {
                                            failedQueue.add(key);
                                        } else if (value == 0) {
                                            emptyQueue.add(key);
                                        } else {
                                            nonEmptyQueue.add(key);
                                        }
                                    });

                                    if (!failedQueue.isEmpty()) {
                                        log.warn("{} queue failed to get size", failedQueue);
                                    }
                                    updateTimestampsWithOverwrite(nonEmptyQueue).onComplete(event -> {
                                        if (event.failed()) {
                                            log.warn("failed to batch update queues timestamp", event.cause());
                                            onDone.accept(event.cause(), null);
                                            return;
                                        }
                                        int updateTimestampsErrorCounter = countErrorsAndNulls(event.result());
                                        if (updateTimestampsErrorCounter > 0) {
                                            log.warn("() queue failed to update queues timestamp", event.cause());
                                        }
                                        batchRefreshRegistration(nonEmptyQueue).onComplete(event2 -> {
                                            if (event2.failed()) {
                                                log.warn("failed to batch update queues registration", event2.cause());
                                                onDone.accept(event2.cause(), null);
                                                return;
                                            }

                                            int refreshRegistrationErrorCounter = countErrorsAndNulls(event2.result());
                                            if (refreshRegistrationErrorCounter > 0) {
                                                log.warn("() queue failed to update queues registration", event2.cause());
                                            }
                                            List<Future<?>> notifyConsumerFutures = new ArrayList<>();
                                            nonEmptyQueue.forEach(queueName -> {
                                                notifyConsumerFutures.add(notifyConsumer(queueName).onComplete(notifyConsumerEvent -> {
                                                    if (notifyConsumerEvent.failed()) log.warn("TODO error handling",
                                                            exceptionFactory.newException("notifyConsumer(" + queueName + ") failed",
                                                                    notifyConsumerEvent.cause()));
                                                }));
                                            });

                                            // reset queues for not exits or empty queue
                                            emptyQueue.forEach(queueName -> {
                                                queueStatsService.dequeueStatisticMarkedForRemoval(queueName);
                                                queueStatisticsCollector.resetQueueFailureStatistics(queueName);
                                            });
                                            Future.all(notifyConsumerFutures).onComplete(event1 -> {
                                                    if (event1.failed()) {
                                                        log.warn("failed to notifyConsumer", event1.cause());
                                                    }
                                                onDone.accept(null, null);
                                            });
                                        });
                                    });
                                }
                            });
                            return iterator.hasNext();
                        }

                        @Override
                        public boolean onError(Throwable ex, Iterator<List<String>> iterator) {
                            log.warn("Failed while processing queue batch", exceptionFactory.newException(ex));
                            return true; // true, keep going with other queues batch in queue.
                        }

                        @Override
                        public void onDone(Iterator<List<String>> iterator) {
                            log.debug("all queue items time used is {} ms", System.currentTimeMillis() - startTs);
                            removeOldQueues(limit).onComplete(removeOldQueuesEvent -> {
                                if (removeOldQueuesEvent.failed() && log.isWarnEnabled()) {
                                    log.warn("TODO error handling", exceptionFactory.newException(
                                            "removeOldQueues(" + limit + ") failed", removeOldQueuesEvent.cause()));
                                }
                                // Mark this composition step as completed.
                                promise.complete();
                            });
                        }
                    });

                    return promise.future();
                });
    }

    /**
     * check the queue list size in a batch command
     * @param queueNameBatch a set of queues name will check
     * @return a Map continues queue name and result, null means error
     */
    Future<Map<String, Integer>> batchCheckQueuesSize(List<String> queueNameBatch) {
        if (queueNameBatch.isEmpty()) {
            return Future.succeededFuture();
        }
        List<Request> requests = new ArrayList<>(queueNameBatch.size());
        Map<String, Integer> reslutMap = new HashMap<>();
        for (String queueName : queueNameBatch) {
            requests.add(Request.cmd(Command.LLEN).arg(keyspaceHelper.getQueuesPrefix() + queueName));
        }
        return redisService.batch(requests).compose(responses -> {
            for (int i = 0; i < queueNameBatch.size(); i++) {
                Response response = responses.get(i);
                String queueName = queueNameBatch.get(i);
                if (response != null && response.type() == ResponseType.NUMBER) {
                    reslutMap.put(queueName, response.toInteger());
                } else {
                    reslutMap.put(queueName, null);
                    log.error("RedisQues is unable to check existence of queue {}, response {}", queueName, response,
                                exceptionFactory.newException("redisAPI.llen() failed"));
                }
            }
            return Future.succeededFuture(reslutMap);
        });
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queueName name of the queue
     */
    public Future<Response> updateTimestamp(final String queueName) {
        final Promise<Response> promise = Promise.promise();

        long ts = System.currentTimeMillis();
        log.trace("RedisQues update timestamp for queue: {} to: {}", queueName, ts);
        redisService.zadd(keyspaceHelper.getQueuesKey(), queueName, String.valueOf(ts))
                .onSuccess(promise::complete)
                .onFailure(throwable -> {
                    log.warn("Redis: Error in updateTimestamp", throwable);
                    promise.fail(throwable);
                });
        return promise.future();
    }

    /**
     * Store the queue names in a sorted set with the current date as score, if exist will update the score
     *
     * @param queueNames list of queue name
     */
    public Future<List<Response>> updateTimestampsWithOverwrite(final List<String> queueNames) {
        if (queueNames == null || queueNames.isEmpty()) {
            log.debug("Got empty queue names for update timestamps");
            return Future.succeededFuture(new ArrayList<>());
        }

        final String queuesKey = keyspaceHelper.getQueuesKey();
        final long score = currentTimeMillis();

        List<Request> requests = new ArrayList<>(queueNames.size());
        for (String queueName : queueNames) {
            requests.add(Request.cmd(Command.ZADD)
                    .arg(queuesKey)
                    .arg("CH")  // update the timestamp
                    .arg(score)
                    .arg(queueName));
        }

        return redisService.batch(requests);
    }

    /**
     * count how many error in a batch request
     * @param batchResponse
     * @return number of error
     */
    int countErrorsAndNulls(List<Response> batchResponse) {
        if (batchResponse == null) {
            return 1;
        }

        int count = 0;
        for (Response item : batchResponse) {
            if (item == null || item.type() == ResponseType.ERROR) {
                count++;
            }
        }
        return count;
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
        unregisterAll(Arrays.asList(consumersMessageConsumer, uidMessageConsumer, notifyConsumer))
                .onComplete(event -> {
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

    public Future<Void> notifyConsumer(final String queueName) {
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
            } else if (!aliveConsumers.contains(consumer)) {
                log.info("RedisQues consumer {} of queue {} does not exist.", consumer, queueName);
                redisService.del(Collections.singletonList(key)).onComplete(result -> {
                    if (result.failed()) {
                        log.warn("Failed to remove consumer '{}'", key, exceptionFactory.newException(result.cause()));
                    } else {
                        if (result.result() != null && result.result().toInteger() == 1) {
                            log.info("consumer key {} removed", key);
                            // need find a new consumer for this queue, let's make a peer become consumer
                            log.debug("RedisQues Sending new registration request for queue {}", queueName);
                            eb.send(keyspaceHelper.getConsumersAddress(), queueName);
                        } else {
                            log.info("Can't delete consumer key {}, result {}, it may delete by another consumer, skip.", key, result.result().toInteger());
                        }
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
