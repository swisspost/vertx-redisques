package org.swisspush.redisques;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ActiveQueueService {
    private static final Logger log = LoggerFactory.getLogger(ActiveQueueService.class);

    private final RedisquesConfigurationProvider configurationProvider;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final RedisProvider redisProvider;
    private final PeriodicSkipScheduler periodicSkipScheduler;
    private final UpperBoundParallel upperBoundParallel;
    private final QueueStatisticsCollector queueStatisticsCollector;
    // The queues this verticle is listening to
    private final Map<String, RedisQues.QueueState> myQueues = new ConcurrentHashMap<>();
    private final Semaphore redisMonitoringReqQuota;
    private final String queuesKey;
    private final String myUID;
    private final QueueRegistrationService queueRegistrationService;

    ActiveQueueService(Vertx vertx, RedisquesConfigurationProvider configurationProvider,
                       QueueRegistrationService queueRegistrationService,
                       String queuesKey, String myUID,
                       RedisQuesExceptionFactory exceptionFactory, RedisProvider redisProvider,
                       QueueStatisticsCollector queueStatisticsCollector,
                       Semaphore redisMonitoringReqQuota) {

        this.configurationProvider = Objects.requireNonNull(configurationProvider);
        this.exceptionFactory = Objects.requireNonNull(exceptionFactory);
        this.redisProvider = Objects.requireNonNull(redisProvider);
        this.queueStatisticsCollector = Objects.requireNonNull(queueStatisticsCollector);
        this.redisMonitoringReqQuota = Objects.requireNonNull(redisMonitoringReqQuota);
        this.queuesKey = queuesKey;
        this.myUID = myUID;
        this.queueRegistrationService = Objects.requireNonNull(queueRegistrationService);

        this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
        this.upperBoundParallel = new UpperBoundParallel(vertx, exceptionFactory);
    }

    Map<String, RedisQues.QueueState> getQueuesState(){
        return myQueues;
    }

    RedisQues.QueueState getQueueState(String queueName){
        return myQueues.get(queueName);
    }

    void putQueueState(String queueName, RedisQues.QueueState queueState){
        myQueues.put(queueName, queueState);
    }

    void removeQueueState(String queueName){
        myQueues.remove(queueName);
    }

    void registerActiveQueueRegistrationRefresh() {
        // Periodic refresh of my registrations on active queues.
        var periodMs = configurationProvider.configuration().getRefreshPeriod() * 1000L;
        periodicSkipScheduler.setPeriodic(periodMs, "registerActiveQueueRegistrationRefresh", new Consumer<Runnable>() {
            Iterator<Map.Entry<String, RedisQues.QueueState>> iter;
            @Override public void accept(Runnable onPeriodicDone) {
                // Need a copy to prevent concurrent modification issuses.
                iter = new HashMap<>(myQueues).entrySet().iterator();
                // Trigger only a limitted amount of requests in parallel.
                upperBoundParallel.request(redisMonitoringReqQuota, iter, new UpperBoundParallel.Mentor<>() {
                    @Override public boolean runOneMore(BiConsumer<Throwable, Void> onQueueDone, Iterator<Map.Entry<String, RedisQues.QueueState>> iter) {
                        handleNextQueueOfInterest(onQueueDone);
                        return iter.hasNext();
                    }
                    @Override public boolean onError(Throwable ex, Iterator<Map.Entry<String, RedisQues.QueueState>> iter) {
                        if (log.isWarnEnabled()) log.warn("TODO error handling", exceptionFactory.newException(ex));
                        return false;
                    }
                    @Override public void onDone(Iterator<Map.Entry<String, RedisQues.QueueState>> iter) {
                        onPeriodicDone.run();
                    }
                });
            }
            void handleNextQueueOfInterest(BiConsumer<Throwable, Void> onQueueDone) {
                while (iter.hasNext()) {
                    var entry = iter.next();
                    if (entry.getValue() != RedisQues.QueueState.CONSUMING) continue;
                    checkIfImStillTheRegisteredConsumer(entry.getKey(), onQueueDone);
                    return;
                }
                // no entry found. we're done.
                onQueueDone.accept(null, null);
            }
            void checkIfImStillTheRegisteredConsumer(String queue, BiConsumer<Throwable, Void> onDone) {
                // Check if I am still the registered consumer
                String consumerKey = queueRegistrationService.getConsumerKey(queue);
                log.trace("RedisQues refresh queues get: {}", consumerKey);
                redisProvider.redis().onComplete( ev1 -> {
                    if (ev1.failed()) {
                        onDone.accept(exceptionFactory.newException("redisProvider.redis() failed", ev1.cause()), null);
                        return;
                    }
                    var redisAPI = ev1.result();
                    redisAPI.get(consumerKey, getConsumerEvent -> {
                        if (getConsumerEvent.failed()) {
                            Throwable ex = exceptionFactory.newException(
                                    "Failed to get queue consumer for queue '" + queue + "'", getConsumerEvent.cause());
                            assert ex != null;
                            onDone.accept(ex, null);
                            return;
                        }
                        final String consumer = Objects.toString(getConsumerEvent.result(), "");
                        if (myUID.equals(consumer)) {
                            log.debug("RedisQues Periodic consumer refresh for active queue {}", queue);
                            queueRegistrationService.refreshConsumerRegistration(queue, ev -> {
                                if (ev.failed()) {
                                    onDone.accept(exceptionFactory.newException("TODO error handling", ev.cause()), null);
                                    return;
                                }
                                queueRegistrationService.updateTimestamp(queuesKey, queue, ev3 -> {
                                    Throwable ex = ev3.succeeded() ? null : exceptionFactory.newException(
                                            "updateTimestamp(" + queue + ") failed", ev3.cause());
                                    onDone.accept(ex, null);
                                });
                            });
                        } else {
                            log.debug("RedisQues Removing queue {} from the list", queue);
                            myQueues.remove(queue);
                            queueStatisticsCollector.resetQueueFailureStatistics(queue, onDone);
                        }
                    });
                });
            }
        });
    }
}
