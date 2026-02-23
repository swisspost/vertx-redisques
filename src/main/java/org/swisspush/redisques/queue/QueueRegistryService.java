package org.swisspush.redisques.queue;

import com.google.common.base.Strings;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.QueueStatsService;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;

public class QueueRegistryService {
    private static final Logger log = LoggerFactory.getLogger(QueueRegistryService.class);
    public static final long LOAD_BALANCE_SCORE_NOT_VALID = -1L;
    private final RedisService redisService;
    private final RedisquesConfigurationProvider configurationProvider;
    private final MessageConsumer<String> consumersMessageConsumer;
    private final MessageConsumer<String> consumersMyMessageConsumer;
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
    private Long lastLoadScore;
    protected Map<String, Long> aliveConsumers = new ConcurrentHashMap<>();


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
        lastLoadScore = null;
        // Handles registration requests
        consumersMessageConsumer = vertx.eventBus().consumer(keyspaceHelper.getConsumersAddress(), this::handleRegistrationRequest);
        consumersMyMessageConsumer = vertx.eventBus().consumer(keyspaceHelper.getMyConsumersAddress(), this::handleRegistrationRequest);
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

        // register my self into alive consumer first.
        registerKeepConsumerAlive();
        registerQueueCheck();
        registerMyqueuesCleanup();
        registerActiveQueueRegistrationRefresh();
        registerNotExpiredQueueCheck();
        registerCalculateLoadScore();
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

    /**
     * get a target if we need forward load to it
     * @param scoreList current load score of all instances
     * @param currentNodeId id of current node
     * @param allowedMargin margin to keep load not swap between load too often, in range of 0-100
     * @return a UUID of instance, if forward is needed, null means no matched target
     */
    @Nullable
    String findRebalanceTarget(
            Map<String, Long> scoreList, String currentNodeId, long currentNodeLoadScore, int allowedMargin) {
        Map<String, Long> localScoreList = new HashMap<>(scoreList);
        if (allowedMargin > 100 || allowedMargin < 0){
            throw new IllegalArgumentException("Margin must in range of 0-100");
        }

        if (scoreList.size() <= 1) {
            log.debug("score list size is {}, not enough to continue", scoreList.size());
            return null;
        }
        // update currentNodeLoad to the newest
        localScoreList.put(currentNodeId, currentNodeLoadScore);
        if (localScoreList.values().stream().anyMatch(v -> v < 0)) {
            log.info("not all instances reported his score, skips");
            return null;
        }

        long totalScore = 0;
        for (long w : localScoreList.values()) totalScore += w;

        final double avgScore = (double) totalScore / localScoreList.size();
        final double marginPercent = (double) allowedMargin / 100D;
        final double margin = avgScore * marginPercent;

        final long currentNodeLoad = localScoreList.getOrDefault(currentNodeId, 0L);

        if (currentNodeLoad <= avgScore + margin) {
            log.debug("current is not overloaded, skip");
            return null;
        }

        String bestNode = null;
        double bestScore = Double.MAX_VALUE;
        for (Map.Entry<String, Long> e : localScoreList.entrySet()) {
            String node = e.getKey();
            final long load = e.getValue();

            if (node.equals(currentNodeId)) {
                continue;
            }
            if (load >= avgScore - margin) {
                continue;
            }
            double futureLoad = (load + currentNodeLoad) / 2.0;
            double score = Math.abs(futureLoad - avgScore);

            if (score < bestScore) {
                bestScore = score;
                bestNode = node;
            }
        }

        return bestNode;
    }

    /**
     * Get a weight score base on queue count and total queue items
     * @param queueNameSet a list of queue name will use to calculates the weight score
     * @param queueSetCountWeight per queue weight
     * @param queueItemCountWeight per item weight
     * @return
     */
    Future<Long> getMyLoadScore(Set<String> queueNameSet, long queueSetCountWeight, long queueItemCountWeight) {
        Promise<Long> promise = Promise.promise();

        List<Future<Long>> futures = new ArrayList<>();
        queueNameSet.forEach(key -> {
            Promise<Long> llenPomise = Promise.promise();
            redisService.llen(keyspaceHelper.getQueuesPrefix() + key).onComplete(asyncResult -> {
                if (asyncResult.succeeded()) {
                    llenPomise.complete(asyncResult.result().toLong());
                } else {
                    llenPomise.fail(asyncResult.cause());
                }
            });

            futures.add(llenPomise.future());
        });

        final int queueSize = queueNameSet.size();
        Future.all(futures)
                .map(cf -> {
                    long sum = 0;
                    for (int i = 0; i < cf.size(); i++) {
                        sum += (long)cf.resultAt(i);
                    }
                    return sum;
                })
                .onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                log.warn("failed to get total items size from myQueue");
                promise.fail(asyncResult.cause());
            } else {
               promise.complete(queueSetCountWeight * queueSize + queueItemCountWeight * asyncResult.result());
            }
        });
        return promise.future();
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

    private Future<HashSet<String>> getAliveConsumers() {
        final Promise<HashSet<String>> promise = Promise.promise();
        final HashSet<String> consumerSet = new HashSet<>();
        // add self first
        consumerSet.add(keyspaceHelper.getVerticleUid());
        // don't have many keys here, so get all at once
        redisService.keys(keyspaceHelper.getAliveConsumersPrefix() + "*").onComplete(keysResult -> {
            if (keysResult.failed()) {
                log.warn("failed to get alive consumer list", keysResult.cause());
            } else {
                Response keys = keysResult.result();
                if (keys == null || keys.size() == 0) {
                    log.debug("No alive consumers found");
                    promise.complete(consumerSet);
                    return;
                }
                for (Response response : keys) {
                    consumerSet.add(response.toString().replace(keyspaceHelper.getAliveConsumersPrefix(), ""));
                }
                metrics.setConsumerCounter(consumerSet.size());
                log.debug("{} alive consumers found", consumerSet.size());
                promise.complete(consumerSet);
            }
        });
        return promise.future();
    }

    private String getVerticleUidWithScore() {
        return keyspaceHelper.getVerticleUid() + ":" + getLastScore();
    }

    private long getLastScore() {
        return lastLoadScore == null ? LOAD_BALANCE_SCORE_NOT_VALID : lastLoadScore;
    }

    private void registerKeepConsumerAlive() {
        // initial set, add self into local list first
        aliveConsumers.put(keyspaceHelper.getVerticleUid(), getLastScore());
        // keep key alive 2 times of refresh period
        final long keyLiveTime = getConfiguration().getRefreshPeriod() * 1000L * 2;

        // add self into Redis with expiring time, without wait.
        final String consumerKey = keyspaceHelper.getAliveConsumersPrefix() + keyspaceHelper.getVerticleUid();
        redisService.setNxPx(consumerKey, getVerticleUidWithScore(), false, keyLiveTime)
                .onFailure(e -> log.warn("failed to set initial alive consumer live key for {}", keyspaceHelper.getVerticleUid(), e));

        // update 2 heartbeat timestamp per refresh period
        final long periodMs = Math.max(getConfiguration().getRefreshPeriod() / 2 * 1000L, 1);

        vertx.setPeriodic(periodMs, event -> {
            redisService.setNxPx(consumerKey, getVerticleUidWithScore(), false, keyLiveTime).onComplete(event2 -> {
                if (event2.failed()) {
                    log.warn("failed to update alive consumer live key for {}", keyspaceHelper.getVerticleUid(), event2.cause());
                } else {
                    log.debug("RedisQues consumer {} keep alive updated", keyspaceHelper.getVerticleUid());
                }
                getAliveConsumers().onComplete(event1 -> {
                    if (event1.failed()) {
                        log.warn("failed to get alive consumer list", event1.cause());
                        return;
                    }
                    HashSet<String> newlist = event1.result();
                    List<String> keyList = new ArrayList<>();
                    List<Future<String>> futures = new ArrayList<>();

                    newlist.forEach(key -> {
                        Promise<String> perKeyPromise = Promise.promise();
                        futures.add(perKeyPromise.future());
                        redisService.get(keyspaceHelper.getAliveConsumersPrefix() + key).onComplete(event3 -> {
                            if (event3.failed()) {
                                log.error("Unable to get consumer key for queue {}", key, event3.cause());
                                perKeyPromise.fail(event3.cause());
                                return;
                            }
                            String consumerKey1 = Objects.toString(event3.result(), null);
                            perKeyPromise.complete(consumerKey1);
                        });
                    });

                    Future.all(futures).onComplete(new Handler<AsyncResult<CompositeFuture>>() {
                        @Override
                        public void handle(AsyncResult<CompositeFuture> keysResult) {
                            // add all with LOAD_BALANCE_SCORE_NOT_VALID first
                            newlist.forEach(k -> aliveConsumers.put(k, LOAD_BALANCE_SCORE_NOT_VALID));
                            if (keysResult.failed()) {
                                log.warn("failed to get value of keys: {}", String.join(",", keyList), keysResult.cause());
                            } else {
                                keysResult.result().list().forEach(valueResponse -> {
                                    if (valueResponse != null){
                                        final String[] keyValue = valueResponse.toString().split(":");
                                        aliveConsumers.put(keyValue[0], keyValue.length > 1 ? Integer.parseInt(keyValue[1]) : LOAD_BALANCE_SCORE_NOT_VALID);
                                    }
                                });
                            }
                            // remove older which not in new list
                            aliveConsumers.keySet().retainAll(newlist);
                            // ensure self is inside
                            aliveConsumers.put(keyspaceHelper.getVerticleUid(), getLastScore());
                        }
                    });

                });
            });
        });
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

    private Future<Void> moveLoadToOtherInstanceIfNeeded (){
        String targetUid = findRebalanceTarget(aliveConsumers, keyspaceHelper.getVerticleUid(), lastLoadScore, getConfiguration().getBalancerWeightCompareMargin());
        if (Strings.isNullOrEmpty(targetUid)) {
            log.debug("targetUid is null, reassign not needed");
            return Future.succeededFuture();
        }
        final int maxAllowToMove = getConfiguration().getBalancerMaxReassignQueueCountPerTime();
        final EventBus eb = vertx.eventBus();
        List<Future<String>> futures = new ArrayList<>();
        final Set<Map.Entry<String, QueueProcessingState>> localCopy = new HashSet<>(queueConsumerRunner.getMyQueues().entrySet());
        for (Map.Entry<String, QueueProcessingState> entry : localCopy) {
            String queueName = entry.getKey();
            QueueProcessingState state = entry.getValue();
            if(state.getState() == QueueState.CONSUMING){
                log.debug("Queue {} is consuming, will not try to forward", queueName);
                continue;
            }
            if (futures.size() >= maxAllowToMove) {
                log.debug("Already have {} will forward", futures.size());
                break;
            }
            Promise<String> byQueuePromise = Promise.promise();
            futures.add(byQueuePromise.future());
            // remove queue from myqueue list
            queueConsumerRunner.getMyQueues().remove(queueName);
            // unregister queue
            unregisterQueue(queueName).onComplete(event -> {
                if (event.succeeded()) {
                    // send to new consumer
                    eb.send(keyspaceHelper.getConsumersAddressByUid(targetUid), queueName);
                    log.info("load balancer moved the queue {}, from {} to {}", queueName, keyspaceHelper.getVerticleUid(), targetUid);
                    byQueuePromise.complete(queueName);
                } else {
                    // unregisterQueue never fail.
                    log.warn("Failed to move queue {} to {}. You should not see this!", queueName, state.getState(), event.cause());
                    // just complete unused promise
                    byQueuePromise.complete();
                }
            });
        }

        Promise<Void> promise = Promise.promise();
        Future.all(futures)
                .onComplete(asyncResult -> {
                    if (asyncResult.failed()) {
                        log.warn("failed to balancing the load, will try again later", asyncResult.cause());
                        promise.complete();
                    } else {
                        promise.complete();
                    }
                });
        return promise.future();
    }

    private void registerActiveQueueRegistrationRefresh() {
        // Periodic refresh of my registrations on active queues.
        var periodMs = getConfiguration().getRefreshPeriod() * 1000L;
        periodicSkipScheduler.setPeriodic(periodMs, "registerActiveQueueRegistrationRefresh", new Consumer<Runnable>() {
            Iterator<Map.Entry<String, QueueProcessingState>> iter;

            @Override
            public void accept(Runnable onPeriodicDone) {
                moveLoadToOtherInstanceIfNeeded().onComplete(event -> {
                    // Need a copy to prevent concurrent modification issues.
                    iter = getSortedMyQueueClone(queueConsumerRunner.getMyQueues()).entrySet().iterator();
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
                            // just continue to refresh next one
                            return true;
                        }

                        @Override
                        public void onDone(Iterator<Map.Entry<String, QueueProcessingState>> iter) {
                            onPeriodicDone.run();
                        }
                    });
                });
            }

            void refreshConsumerRegistration(BiConsumer<Throwable, Void> onQueueDone) {
                while (iter.hasNext()) {
                    var entry = iter.next();
                    var state = entry.getValue().getState();
                    if (state != QueueState.CONSUMING) {
                        log.trace("nothing to be done for this entry because state is: {}", state);
                        continue;
                    }
                    /* MUST only trigger *ONE* entry, with that call, we do exactly this. We
                     * also delegate the `onDone()` callback to our callee, so we also are NOT
                     * responsible to call that anymore ourself, therefore we're ready to return. */
                    checkIfImStillTheRegisteredConsumer(entry.getKey(), onQueueDone);
                    return;
                }
                /* did NOT trigger any entry above. So we MUST signal the completion ourself. */
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
                            updateTimestamp(queue).onComplete(ev3 -> {
                                Throwable ex = ev3.succeeded() ? null : exceptionFactory.newException(
                                        "updateTimestamp(" + queue + ") failed", ev3.cause());
                                onDone.accept(ex, null);
                            });
                        });
                    } else {
                        log.debug("RedisQues Removing queue {} from the list", queue);
                        queueConsumerRunner.getMyQueues().remove(queue);
                        // This queue is not owned by this instance; removing it from the local dequeue statistics cache.
                        queueStatsService.dequeueStatisticRemoveFromLocal(queue);
                        queueStatisticsCollector.resetQueueFailureStatistics(queue, onDone);
                    }
                });
            }
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

                        Handler<Void> checkNextHandler = event -> {
                            final int checkDelayMs = getConfiguration().getQueueCheckDelayTimeMs();
                            if (checkDelayMs > 0) {
                                vertx.setTimer(checkDelayMs, timerId -> {
                                    onDone.accept(null, null);
                                });
                            } else {
                                onDone.accept(null, null);
                            }
                        };

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
                                    checkNextHandler.handle(null);
                                });
                            });
                        };

                        redisService.exists(Collections.singletonList(key)).onComplete(event -> {
                            if (event.failed() || event.result() == null) {
                                log.error("RedisQues is unable to check existence of queue {}", queueName,
                                        exceptionFactory.newException("redisAPI.exists(" + key + ") failed", event.cause()));
                                checkNextHandler.handle(null);
                                return;
                            }
                            if (event.result().toLong() == 1) {
                                log.trace("Updating queue timestamp for queue '{}'", queueName);
                                // If not empty, update the queue timestamp to keep it in the sorted set.
                                updateTimestamp(queueName).onComplete(upTsResult -> {
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
        unregisterAll(Arrays.asList(consumersMessageConsumer, consumersMyMessageConsumer, uidMessageConsumer,
                refreshRegistrationConsumer, notifyConsumer)).onComplete(event -> {
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
            } else if (!aliveConsumers.keySet().contains(consumer)) {
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

    /**
     * calculate current instance's load weight score
     */
    private void registerCalculateLoadScore() {
        final long queueSetCountWeight = getConfiguration().getBalancerQueueCountWeight();
        final long queueItemCountWeight = getConfiguration().getBalancerQueueItemCountWeight();
        vertx.setPeriodic(getConfiguration().getBalancerLocalScoreUpdateIntervalSec() * 1000L, event -> {
            if (!getConfiguration().isBalancerEnabled()) {
                return;
            }
            getMyLoadScore(queueConsumerRunner.getMyQueues().keySet(), queueSetCountWeight, queueItemCountWeight).onComplete(event1 -> {
                if (event1.failed()) {
                    log.error("Failed to get load score");
                    lastLoadScore = null;
                } else {
                    log.debug("Load score: {}", event1.result());
                    lastLoadScore = event1.result();
                    final String metricsAddress = getConfiguration().getPublishMetricsAddress();
                    if (Strings.isNullOrEmpty(metricsAddress)) {
                        return;
                    }
                    vertx.eventBus().send(
                            metricsAddress,
                            new JsonObject()
                                    .put("name", "redisques.balancer.instance." + keyspaceHelper.getInstanceIndex() + ".score")
                                    .put("action", "set")
                                    .put("n", lastLoadScore));
                }
            });
        });
    }
}
