package org.swisspush.redisques;

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

    private static final Logger log = LoggerFactory.getLogger(RedisQues.class);
    private MemoryUsageProvider memoryUsageProvider;
    private RedisquesConfigurationProvider configurationProvider;
    private RedisProvider redisProvider;
    private RedisQuesExceptionFactory exceptionFactory;
    private Semaphore redisMonitoringReqQuota;
    private Semaphore checkQueueRequestsQuota;
    private Semaphore queueStatsRequestQuota;
    private Semaphore getQueuesItemsCountRedisRequestQuota;

    RedisQuesBuilder() {
        // Private, as clients should use "RedisQues.builder()" and not this class here directly.
    }

    public org.swisspush.redisques.RedisQuesBuilder withMemoryUsageProvider(MemoryUsageProvider memoryUsageProvider) {
        this.memoryUsageProvider = memoryUsageProvider;
        return this;
    }

    public org.swisspush.redisques.RedisQuesBuilder withRedisquesRedisquesConfigurationProvider(RedisquesConfigurationProvider configurationProvider) {
        this.configurationProvider = configurationProvider;
        return this;
    }

    public org.swisspush.redisques.RedisQuesBuilder withRedisProvider(RedisProvider redisProvider) {
        this.redisProvider = redisProvider;
        return this;
    }

    public org.swisspush.redisques.RedisQuesBuilder withExceptionFactory(RedisQuesExceptionFactory exceptionFactory) {
        this.exceptionFactory = exceptionFactory;
        return this;
    }

    /**
     * How many redis requests monitoring related component will trigger
     * simultaneously. One of those components for example is
     * {@link QueueStatisticsCollector}.
     */
    public org.swisspush.redisques.RedisQuesBuilder withRedisMonitoringReqQuota(Semaphore quota) {
        this.redisMonitoringReqQuota = quota;
        return this;
    }

    /**
     * How many redis requests {@link QueueWatcherService#checkQueues()} will trigger
     * simultaneously.
     */
    public org.swisspush.redisques.RedisQuesBuilder withCheckQueueRequestsQuota(Semaphore quota) {
        this.checkQueueRequestsQuota = quota;
        return this;
    }

    /**
     * How many incoming requests {@link QueueStatsService} will accept
     * simultaneously.
     */
    public org.swisspush.redisques.RedisQuesBuilder withQueueStatsRequestQuota(Semaphore quota) {
        this.queueStatsRequestQuota = quota;
        return this;
    }

    /**
     * How many simultaneous redis requests will be performed maximally for
     * {@link org.swisspush.redisques.handler.GetQueuesItemsCountHandler} requests.
     */
    public org.swisspush.redisques.RedisQuesBuilder withGetQueuesItemsCountRedisRequestQuota(Semaphore quota) {
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
                redisMonitoringReqQuota, checkQueueRequestsQuota, queueStatsRequestQuota,
                getQueuesItemsCountRedisRequestQuota);
    }
}
