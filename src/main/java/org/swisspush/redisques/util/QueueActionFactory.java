package org.swisspush.redisques.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.swisspush.redisques.action.*;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import java.util.List;
import java.util.concurrent.Semaphore;

public class QueueActionFactory {

    private final RedisProvider redisProvider;
    private final Vertx vertx;
    private final Logger log;
    private final String address;
    private final String queuesKey;
    private final String queuesPrefix;
    private final String consumersPrefix;
    private final String locksKey;
    private final List<QueueConfiguration> queueConfigurations;
    private final QueueStatisticsCollector queueStatisticsCollector;
    private final int memoryUsageLimitPercent;
    private final MeterRegistry meterRegistry;
    private final String metricsIdentifier;
    private final MemoryUsageProvider memoryUsageProvider;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final Semaphore getQueuesItemsCountRedisRequestQuota;

    private final RedisquesConfigurationProvider configurationProvider;

    public QueueActionFactory(
        RedisProvider redisProvider,
        Vertx vertx,
        Logger log,
        String queuesKey,
        String queuesPrefix,
        String consumersPrefix,
        String locksKey,
        MemoryUsageProvider memoryUsageProvider,
        QueueStatisticsCollector queueStatisticsCollector,
        RedisQuesExceptionFactory exceptionFactory,
        RedisquesConfigurationProvider configurationProvider,
        Semaphore getQueuesItemsCountRedisRequestQuota,
        MeterRegistry meterRegistry
    ) {
        this.redisProvider = redisProvider;
        this.vertx = vertx;
        this.log = log;
        this.queuesKey = queuesKey;
        this.queuesPrefix = queuesPrefix;
        this.consumersPrefix = consumersPrefix;
        this.locksKey = locksKey;
        this.memoryUsageProvider = memoryUsageProvider;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.exceptionFactory = exceptionFactory;
        this.configurationProvider = configurationProvider;
        this.address = configurationProvider.configuration().getAddress();
        this.queueConfigurations = configurationProvider.configuration().getQueueConfigurations();
        this.memoryUsageLimitPercent = configurationProvider.configuration().getMemoryUsageLimitPercent();
        this.getQueuesItemsCountRedisRequestQuota = getQueuesItemsCountRedisRequestQuota;
        this.meterRegistry = meterRegistry;

        metricsIdentifier = configurationProvider.configuration().getMicrometerMetricsIdentifier();
    }

    public QueueAction buildQueueAction(RedisquesAPI.QueueOperation queueOperation){
        switch (queueOperation){
            case addQueueItem:
                return new AddQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteQueueItem:
                return new DeleteQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteAllQueueItems:
                return new DeleteAllQueueItemsAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                    consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case bulkDeleteQueues:
                return new BulkDeleteQueuesAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case replaceQueueItem:
                return new ReplaceQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueueItem:
                return new GetQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueueItems:
                return new GetQueueItemsAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueues:
                return new GetQueuesAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesCount:
                return new GetQueuesCountAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueueItemsCount:
                return new GetQueueItemsCountAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesItemsCount:
                return new GetQueuesItemsCountAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory,
                        getQueuesItemsCountRedisRequestQuota, queueStatisticsCollector, log);
            case enqueue:
                return new EnqueueAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log,
                        memoryUsageProvider, memoryUsageLimitPercent, meterRegistry, metricsIdentifier);
            case lockedEnqueue:
                return new LockedEnqueueAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log,
                        memoryUsageProvider, memoryUsageLimitPercent, meterRegistry, metricsIdentifier);
            case getLock:
                return new GetLockAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case putLock:
                return new PutLockAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case bulkPutLocks:
                return new BulkPutLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getAllLocks:
                return new GetAllLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteLock:
                return new DeleteLockAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case bulkDeleteLocks:
                return new BulkDeleteLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteAllLocks:
                return new DeleteAllLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesSpeed:
                return new GetQueuesSpeedAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesStatistics:
                return new GetQueuesStatisticsAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case setConfiguration:
                return new SetConfigurationAction(configurationProvider, log);
            case getConfiguration:
                return new GetConfigurationAction(configurationProvider);
            default:
                return new UnsupportedAction(log);
        }
    }
    
    public QueueAction buildUnsupportedAction() {
        return new UnsupportedAction(log);
    }
}
