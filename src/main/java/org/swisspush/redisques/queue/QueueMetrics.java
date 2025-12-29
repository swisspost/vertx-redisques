package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueueMetrics {
    private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);
    private final RedisquesConfigurationProvider configurationProvider;
    private final Map<String, LongTaskTimerSamplePair> perQueueMetrics = new ConcurrentHashMap<>();
    private final Vertx vertx;
    private final KeyspaceHelper keyspaceHelper;
    private MeterRegistry meterRegistry;
    private Counter dequeueCounter;
    private Counter consumerCounter;
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
        this.lock = new RedisBasedLock(redisService, exceptionFactory);
    }

    private void createMetricForQueueIfNeeded(String queueName) {
        var cfg = configurationProvider.configuration();
        if (!(cfg.getMicrometerMetricsEnabled() && cfg.getMicrometerPerQueueMetricEnabled())) {
            return;
        }
        LongTaskTimer task = LongTaskTimer.builder(MetricMeter.QUEUE_CONSUMER_LIFE_CYCLE.getId())
                .description(MetricMeter.QUEUE_CONSUMER_LIFE_CYCLE.getDescription())
                .tag(MetricTags.IDENTIFIER.getId(), cfg.getMicrometerMetricsIdentifier())
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
        var cfg = configurationProvider.configuration();
        if (!(cfg.getMicrometerMetricsEnabled() && cfg.getMicrometerPerQueueMetricEnabled())) {
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
        var cfg = configurationProvider.configuration();
        if (!(cfg.getMicrometerMetricsEnabled() && cfg.getMicrometerPerQueueMetricEnabled())) {
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
        var cfg = configurationProvider.configuration();
        if (!(cfg.getMicrometerMetricsEnabled() && cfg.getMicrometerPerQueueMetricEnabled())) {
            return;
        }
        perQueueMetrics.computeIfPresent(queueName, (key, longTaskTimerSamplePair) -> {
            // Queue have been Deregistered, remove this LongTaskTimer
            longTaskTimerSamplePair.getSample().stop();
            return null;
        });
        perQueueMetrics.remove(queueName);
    }

    public void initMicrometerMetrics() {
        var cfg = configurationProvider.configuration();
        if(cfg.getMicrometerMetricsEnabled()) {
            if (meterRegistry == null) {
                meterRegistry = BackendRegistries.getDefaultNow();
            }

            String metricsIdentifier = cfg.getMicrometerMetricsIdentifier();
            dequeueCounter = Counter.builder(MetricMeter.DEQUEUE.getId())
                    .description(MetricMeter.DEQUEUE.getDescription()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).register(meterRegistry);

            consumerCounter = Counter.builder(MetricMeter.QUEUE_CONSUMER_COUNT.getId())
                    .description(MetricMeter.QUEUE_CONSUMER_COUNT.getDescription()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).register(meterRegistry);

            int metricRefreshPeriod = cfg.getMetricRefreshPeriod();
            if (metricRefreshPeriod > 0) {
                String identifier = cfg.getMicrometerMetricsIdentifier();
                metricsCollector = new MetricsCollector(vertx, keyspaceHelper, identifier, meterRegistry, lock, metricRefreshPeriod);
                new MetricsCollectorScheduler(vertx, metricsCollector, metricRefreshPeriod);
            }
        }
    }
}
