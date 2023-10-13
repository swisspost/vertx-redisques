package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.swisspush.redisques.action.*;

import java.util.List;

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
    private final MemoryUsageProvider memoryUsageProvider;

    private final RedisquesConfigurationProvider configurationProvider;

    public QueueActionFactory(RedisProvider redisProvider, Vertx vertx, Logger log,
                              String queuesKey, String queuesPrefix, String consumersPrefix,
                              String locksKey, QueueStatisticsCollector queueStatisticsCollector, MemoryUsageProvider memoryUsageProvider,
                              RedisquesConfigurationProvider configurationProvider) {
        this.redisProvider = redisProvider;
        this.vertx = vertx;
        this.log = log;
        this.queuesKey = queuesKey;
        this.queuesPrefix = queuesPrefix;
        this.consumersPrefix = consumersPrefix;
        this.locksKey = locksKey;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.memoryUsageProvider = memoryUsageProvider;
        this.configurationProvider = configurationProvider;

        this.address = configurationProvider.configuration().getAddress();
        this.queueConfigurations = configurationProvider.configuration().getQueueConfigurations();
        this.memoryUsageLimitPercent = configurationProvider.configuration().getMemoryUsageLimitPercent();
    }

    public QueueAction buildQueueAction(RedisquesAPI.QueueOperation queueOperation){
        switch (queueOperation){
            case addQueueItem:
                return new AddQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteQueueItem:
                return new DeleteQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteAllQueueItems:
                return new DeleteAllQueueItemsAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                    consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case bulkDeleteQueues:
                return new BulkDeleteQueuesAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case replaceQueueItem:
                return new ReplaceQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueueItem:
                return new GetQueueItemAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueueItems:
                return new GetQueueItemsAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueues:
                return new GetQueuesAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesCount:
                return new GetQueuesCountAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueueItemsCount:
                return new GetQueueItemsCountAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesItemsCount:
                return new GetQueuesItemsCountAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case enqueue:
                return new EnqueueAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log, memoryUsageProvider,
                        memoryUsageLimitPercent);
            case lockedEnqueue:
                return new LockedEnqueueAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log, memoryUsageProvider,
                        memoryUsageLimitPercent);
            case getLock:
                return new GetLockAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case putLock:
                return new PutLockAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case bulkPutLocks:
                return new BulkPutLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getAllLocks:
                return new GetAllLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteLock:
                return new DeleteLockAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case bulkDeleteLocks:
                return new BulkDeleteLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteAllLocks:
                return new DeleteAllLocksAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesSpeed:
                return new GetQueuesSpeedAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesStatistics:
                return new GetQueuesStatisticsAction(vertx, redisProvider, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
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
