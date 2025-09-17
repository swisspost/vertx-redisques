package org.swisspush.redisques;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.MemoryUsageProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.concurrent.Semaphore;

import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newThriftyExceptionFactory;

public class RedisQuesBuilder {
    private static final Logger log = LoggerFactory.getLogger(RedisQuesBuilder.class);
    private MemoryUsageProvider memoryUsageProvider;
    private RedisquesConfigurationProvider configurationProvider;
    private RedisProvider redisProvider;
    private RedisQuesExceptionFactory exceptionFactory;
    private MeterRegistry meterRegistry;
    private Semaphore redisMonitoringReqQuota;
    private Semaphore checkQueueRequestsQuota;
    private Semaphore queueStatsRequestQuota;
    private Semaphore getQueuesItemsCountRedisRequestQuota;
    private Semaphore activeQueueRegRefreshReqQuota;

    RedisQuesBuilder() {
        // Private, as clients should use "RedisQues.builder()" and not this class here directly.
    }

    public RedisQuesBuilder withMemoryUsageProvider(MemoryUsageProvider memoryUsageProvider) {
        this.memoryUsageProvider = memoryUsageProvider;
        return this;
    }

    public RedisQuesBuilder withRedisquesRedisquesConfigurationProvider(RedisquesConfigurationProvider configurationProvider) {
        this.configurationProvider = configurationProvider;
        return this;
    }

    public RedisQuesBuilder withRedisProvider(RedisProvider redisProvider) {
        this.redisProvider = redisProvider;
        return this;
    }

    public RedisQuesBuilder withExceptionFactory(RedisQuesExceptionFactory exceptionFactory) {
        this.exceptionFactory = exceptionFactory;
        return this;
    }

    public RedisQuesBuilder withMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        return this;
    }

    /**
     * How many redis requests monitoring related component will trigger
     * simultaneously. One of those components for example is
     * {@link QueueStatisticsCollector}.
     */
    public RedisQuesBuilder withRedisMonitoringReqQuota(Semaphore quota) {
        this.redisMonitoringReqQuota = quota;
        return this;
    }

    /**
     * How many active queues registrations will trigger at once
     */
    public RedisQuesBuilder withActiveQueueRegRefreshReqQuota(Semaphore quota) {
        this.activeQueueRegRefreshReqQuota = quota;
        return this;
    }

    /**
     * How many redis requests {@link RedisQues#checkQueues()} will trigger
     * simultaneously.
     */
    public RedisQuesBuilder withCheckQueueRequestsQuota(Semaphore quota) {
        this.checkQueueRequestsQuota = quota;
        return this;
    }

    /**
     * How many incoming requests {@link QueueStatsService} will accept
     * simultaneously.
     */
    public RedisQuesBuilder withQueueStatsRequestQuota(Semaphore quota) {
        this.queueStatsRequestQuota = quota;
        return this;
    }

    /**
     * How many simultaneous redis requests will be performed maximally for
     * {@link org.swisspush.redisques.handler.GetQueuesItemsCountHandler} requests.
     */
    public RedisQuesBuilder withGetQueuesItemsCountRedisRequestQuota(Semaphore quota) {
        this.getQueuesItemsCountRedisRequestQuota = quota;
        return this;
    }

    public RedisQues build() {
        if (exceptionFactory == null) {
            exceptionFactory = newThriftyExceptionFactory();
        }
        if (redisMonitoringReqQuota == null) {
            redisMonitoringReqQuota = new Semaphore(Integer.MAX_VALUE);
            log.warn("No redis request limit provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
        }
        if (activeQueueRegRefreshReqQuota == null) {
            activeQueueRegRefreshReqQuota = new Semaphore(Integer.MAX_VALUE);
            log.warn("No active queue register refresh limit provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
        }
        if (checkQueueRequestsQuota == null) {
            checkQueueRequestsQuota = new Semaphore(Integer.MAX_VALUE);
            log.warn("No redis check queue limit provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
        }
        if (queueStatsRequestQuota == null) {
            queueStatsRequestQuota = new Semaphore(Integer.MAX_VALUE);
            log.warn("No redis queue stats limit provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
        }
        if (getQueuesItemsCountRedisRequestQuota == null) {
            getQueuesItemsCountRedisRequestQuota = new Semaphore(Integer.MAX_VALUE);
            log.warn("No redis getQueueItemsCount quota provided. Fallback to legacy behavior of {}.", Integer.MAX_VALUE);
        }
        return new RedisQues(memoryUsageProvider, configurationProvider, redisProvider, exceptionFactory,
                redisMonitoringReqQuota, activeQueueRegRefreshReqQuota, checkQueueRequestsQuota, queueStatsRequestQuota,
                getQueuesItemsCountRedisRequestQuota, meterRegistry);
    }
}