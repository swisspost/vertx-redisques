package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.micrometer.backends.BackendRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.lock.Lock;
import org.swisspush.redisques.lock.impl.RedisBasedLock;
import org.swisspush.redisques.metrics.LongTaskTimerSamplePair;
import org.swisspush.redisques.metrics.MetricsCollector;
import org.swisspush.redisques.metrics.MetricsCollectorScheduler;
import org.swisspush.redisques.util.DequeueStatistic;
import org.swisspush.redisques.util.DequeueStatisticCollector;
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.currentTimeMillis;

public class QueueMetrics {
    private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);
    private final RedisquesConfigurationProvider configurationProvider;
    private final Map<String, LongTaskTimerSamplePair> perQueueMetrics = new ConcurrentHashMap<>();
    private final Vertx vertx;
    private final KeyspaceHelper keyspaceHelper;
    private MeterRegistry meterRegistry;
    private Counter dequeueCounter;
    private Counter consumerCounter;
    private Map<String, DequeueStatistic> dequeueStatistic = new ConcurrentHashMap<>();
    private DequeueStatisticCollector dequeueStatisticCollector;
    private QueueStatisticsCollector queueStatisticsCollector;
    private boolean dequeueStatisticEnabled = false;
    MetricsCollector metricsCollector;
    private Lock lock;


    void dequeueCounterIncrement() {
        if (dequeueCounter == null) {
            return;
        }
        dequeueCounter.increment();
    }

    void consumerCounterIncrement(int num) {
        if (consumerCounter == null) {
            return;
        }
        consumerCounter.increment();
    }


    public QueueMetrics(Vertx vertx, KeyspaceHelper keyspaceHelper, RedisService redisService, MeterRegistry meterRegistry, RedisquesConfigurationProvider configurationProvider,
                        RedisQuesExceptionFactory exceptionFactory) {
        this.meterRegistry = meterRegistry;
        this.vertx = vertx;
        this.configurationProvider = configurationProvider;
        this.keyspaceHelper = keyspaceHelper;
        int dequeueStatisticReportIntervalSec = configurationProvider.configuration().getDequeueStatisticReportIntervalSec();
        if (configurationProvider.configuration().isDequeueStatsEnabled()) {
            dequeueStatisticEnabled = true;
            Runnable publisher = newDequeueStatisticPublisher();
            vertx.setPeriodic(1000L * dequeueStatisticReportIntervalSec, time -> publisher.run());
        }
        if (this.dequeueStatisticCollector == null) {
            this.dequeueStatisticCollector = new DequeueStatisticCollector(vertx, dequeueStatisticEnabled);
        }
        this.lock = new RedisBasedLock(redisService, exceptionFactory);
    }

    public void dequeueStatisticSetLastDequeueAttemptTimestamp(String queue, @Nullable Long lastDequeueAttemptTimestamp) {
        if (dequeueStatisticEnabled) {
            dequeueStatistic.computeIfPresent(queue, (s, dequeueStatistic) -> {
                dequeueStatistic.setLastDequeueAttemptTimestamp(lastDequeueAttemptTimestamp);
                return dequeueStatistic;
            });
        }
    }

    public void dequeueStatisticSetLastDequeueSuccessTimestamp(String queue, @Nullable Long lastDequeueSuccessTimestamp) {
        if (dequeueStatisticEnabled) {
            dequeueStatistic.computeIfPresent(queue, (s, dequeueStatistic) -> {
                dequeueStatistic.setLastDequeueSuccessTimestamp(lastDequeueSuccessTimestamp);
                return dequeueStatistic;
            });
        }
    }

    public void dequeueStatisticSetNextDequeueDueTimestamp(String queue, @Nullable Long dueTimestamp) {
        if (dequeueStatisticEnabled) {
            dequeueStatistic.computeIfPresent(queue, (s, dequeueStatistic) -> {
                dequeueStatistic.setNextDequeueDueTimestamp(dueTimestamp);
                return dequeueStatistic;
            });
        }
    }

    public void dequeueStatisticMarkedForRemoval(String queue) {
        if (dequeueStatisticEnabled) {
            dequeueStatistic.computeIfPresent(queue, (s, dequeueStatistic) -> {
                dequeueStatistic.setMarkedForRemoval();
                return dequeueStatistic;
            });
        }
    }

    public void createDequeueStatisticIfMissing(String queueName) {
        dequeueStatistic.computeIfAbsent(queueName, s -> new DequeueStatistic());
    }


    private void createMetricForQueueIfNeeded(String queueName) {
        if (!(configurationProvider.configuration().getMicrometerMetricsEnabled() &&
                configurationProvider.configuration().getMicrometerPerQueueMetricEnabled())
        ) {
            return;
        }
        LongTaskTimer task = LongTaskTimer.builder(MetricMeter.QUEUE_CONSUMER_LIFE_CYCLE.getId())
                .description(MetricMeter.QUEUE_CONSUMER_LIFE_CYCLE.getDescription())
                .tag(MetricTags.IDENTIFIER.getId(), configurationProvider.configuration().getMicrometerMetricsIdentifier())
                .tag(MetricTags.CONSUMER_UID.getId(), keyspaceHelper.getVerticleUid())
                .tag(MetricTags.QUEUE_NAME.getId(), queueName)
                .register(meterRegistry);
        LongTaskTimer.Sample sample = task.start();
        LongTaskTimerSamplePair pair = new LongTaskTimerSamplePair(task, sample);
        perQueueMetrics.put(queueName, pair);
    }

    /**
     * create a timer metric for a queue's consumer, and start a Sample (Consumer Created)
     *
     * @param queueName
     */
    void perQueueMetricsReg(String queueName) {
        if (!(configurationProvider.configuration().getMicrometerMetricsEnabled() &&
                configurationProvider.configuration().getMicrometerPerQueueMetricEnabled())
        ) {
            return;
        }
        createMetricForQueueIfNeeded(queueName);
    }

    /**
     * create a new Sample for timer metric of queue consumer, and stop previous one (Consumer Refresh)
     *
     * @param queueName
     */
    void perQueueMetricsRefresh(String queueName) {
        if (!(configurationProvider.configuration().getMicrometerMetricsEnabled() &&
                configurationProvider.configuration().getMicrometerPerQueueMetricEnabled())
        ) {
            return;
        }
        createMetricForQueueIfNeeded(queueName);
        perQueueMetrics.computeIfPresent(queueName, (key, longTaskTimerSamplePair) -> {
            // A refresh cycle, create a new sample
            longTaskTimerSamplePair.getSample().stop();
            longTaskTimerSamplePair.setSample(longTaskTimerSamplePair.getLongTaskTimer().start());
            return longTaskTimerSamplePair;
        });
    }

    /**
     * stop previous Sample of queue consumer, and remove this timer metric. (Consumer EOL)
     *
     * @param queueName
     */
    void perQueueMetricsRemove(String queueName) {
        if (!(configurationProvider.configuration().getMicrometerMetricsEnabled() &&
                configurationProvider.configuration().getMicrometerPerQueueMetricEnabled())
        ) {
            return;
        }
        perQueueMetrics.computeIfPresent(queueName, (key, longTaskTimerSamplePair) -> {
            // Queue have been Deregistered, remove this LongTaskTimer
            longTaskTimerSamplePair.getSample().stop();
            return null;
        });
        perQueueMetrics.remove(queueName);
    }

    public void initMicrometerMetrics(RedisquesConfiguration modConfig) {
        if(configurationProvider.configuration().getMicrometerMetricsEnabled()) {
            if (meterRegistry == null) {
                meterRegistry = BackendRegistries.getDefaultNow();
            }

            String metricsIdentifier = modConfig.getMicrometerMetricsIdentifier();
            dequeueCounter = Counter.builder(MetricMeter.DEQUEUE.getId())
                    .description(MetricMeter.DEQUEUE.getDescription()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).register(meterRegistry);

            consumerCounter = Counter.builder(MetricMeter.QUEUE_CONSUMER_COUNT.getId())
                    .description(MetricMeter.QUEUE_CONSUMER_COUNT.getDescription()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).register(meterRegistry);

            String address = modConfig.getAddress();
            int metricRefreshPeriod = modConfig.getMetricRefreshPeriod();
            if (metricRefreshPeriod > 0) {
                String identifier = modConfig.getMicrometerMetricsIdentifier();
                metricsCollector = new MetricsCollector(vertx, keyspaceHelper.getVerticleUid(), address, identifier, meterRegistry, lock, metricRefreshPeriod);
                new MetricsCollectorScheduler(vertx, metricsCollector, metricRefreshPeriod);
            }
        }
    }

    public String getMetricsCollectorAddress() {
        return metricsCollector.getAddress();
    }

    class Task {
        private final String queueName;
        private final DequeueStatistic dequeueStatistic;

        Task(String queueName, DequeueStatistic dequeueStatistic) {
            this.queueName = queueName;
            this.dequeueStatistic = dequeueStatistic;
        }

        Future<Void> execute() {
            // switch to a worker thread
            return vertx.executeBlocking(promise -> {
                dequeueStatisticCollector.setDequeueStatistic(queueName, dequeueStatistic).onComplete(event -> {
                    if (event.failed()) {
                        log.error("Future that should always succeed has failed, ignore it", event.cause());
                    }
                    promise.complete();
                });
            });
        }
    }

    private Runnable newDequeueStatisticPublisher() {
        return new Runnable() {
            final AtomicBoolean isRunning = new AtomicBoolean();
            Iterator<Map.Entry<String, DequeueStatistic>> iter;
            long startEpochMs;
            AtomicInteger i = new AtomicInteger();
            int size;

            public void run() {
                if (!isRunning.compareAndSet(false, true)) {
                    log.warn("Previous publish run still in progress at idx {} of {} since {}ms",
                            i, size, currentTimeMillis() - startEpochMs);
                    return;
                }
                try {
                    // Local copy to prevent changes between 'size' and 'iterator' call, plus
                    // to prevent changes of the backing set while we're iterating.
                    Map<String, DequeueStatistic> localCopy = new HashMap<>(dequeueStatistic);
                    size = localCopy.size();
                    iter = localCopy.entrySet().iterator();

                    // use local copy to clean up
                    localCopy.forEach((queueName, dequeueStatistic) -> {
                        if (dequeueStatistic.isMarkedForRemoval()) {
                            QueueMetrics.this.dequeueStatistic.remove(queueName);
                        }
                    });

                    i.set(0);
                    startEpochMs = currentTimeMillis();
                    if (size > 5_000) log.warn("Going to report {} dequeue statistics towards collector", size);
                    else if (size > 500) log.info("Going to report {} dequeue statistics towards collector", size);
                    else log.trace("Going to report {} dequeue statistics towards collector", size);
                } catch (Throwable ex) {
                    isRunning.set(false);
                    throw ex;
                }
                resume();
            }

            void resume() {
                // here we are executing in an event loop thread
                try {
                    List<Task> entryList = new ArrayList<>();
                    while (iter.hasNext()) {
                        var entry = iter.next();
                        var queueName = entry.getKey();
                        var dequeueStatistic = entry.getValue();
                        entryList.add(new Task(queueName, dequeueStatistic));
                    }

                    Future<List<Void>> startFuture = Future.succeededFuture(new ArrayList<>());
                    // chain the futures sequentially to execute tasks
                    Future<List<Void>> resultFuture = entryList.stream()
                            .reduce(startFuture, (future, task) -> future.compose(previousResults -> {
                                // perform asynchronous task
                                return task.execute().compose(taskResult -> {
                                    // append task result to previous results
                                    previousResults.add(taskResult);
                                    i.incrementAndGet();
                                    return Future.succeededFuture(previousResults);
                                });
                            }), (a, b) -> Future.succeededFuture());
                    resultFuture.onComplete(event -> {
                        if (event.failed()) {
                            log.error("publishing dequeue statistics not complete, just continue", event.cause());
                        }
                        if (log.isTraceEnabled()) {
                            log.trace("Done publishing {} dequeue statistics. Took {}ms", i, currentTimeMillis() - startEpochMs);
                        }
                        isRunning.set(false);
                    });
                } catch (Throwable ex) {
                    isRunning.set(false);
                    throw ex;
                }
            }
        };
    }
}
