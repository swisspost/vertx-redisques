package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.action.*;
import org.swisspush.redisques.lua.LuaScriptManager;

import java.util.List;

public class QueueActionFactory {

    private LuaScriptManager luaScriptManager;
    private RedisAPI redisAPI;
    private Vertx vertx;
    private Logger log;
    private String address;
    private String queuesKey;
    private String queuesPrefix;
    private String consumersPrefix;
    private String locksKey;
    private List<QueueConfiguration> queueConfigurations;
    private QueueStatisticsCollector queueStatisticsCollector;
    private int memoryUsageLimitPercent;
    private MemoryUsageProvider memoryUsageProvider;

    private RedisquesConfigurationProvider configurationProvider;

    public QueueActionFactory(LuaScriptManager luaScriptManager, RedisAPI redisAPI, Vertx vertx, Logger log,
                              String queuesKey, String queuesPrefix, String consumersPrefix,
                              String locksKey, QueueStatisticsCollector queueStatisticsCollector, MemoryUsageProvider memoryUsageProvider,
                              RedisquesConfigurationProvider configurationProvider) {
        this.luaScriptManager = luaScriptManager;
        this.redisAPI = redisAPI;
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
                return new AddQueueItemAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteQueueItem:
                return new DeleteQueueItemAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteAllQueueItems:
                return new DeleteAllQueueItemsAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                    consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case bulkDeleteQueues:
                return new BulkDeleteQueuesAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case replaceQueueItem:
                return new ReplaceQueueItemAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueueItem:
                return new GetQueueItemAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueueItems:
                return new GetQueueItemsAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueues:
                return new GetQueuesAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesCount:
                return new GetQueuesCountAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueueItemsCount:
                return new GetQueueItemsCountAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesItemsCount:
                return new GetQueuesItemsCountAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case enqueue:
                return new EnqueueAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log, memoryUsageProvider,
                        memoryUsageLimitPercent);
            case lockedEnqueue:
                return new LockedEnqueueAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log, memoryUsageProvider,
                        memoryUsageLimitPercent);
            case getLock:
                return new GetLockAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case putLock:
                return new PutLockAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case bulkPutLocks:
                return new BulkPutLocksAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getAllLocks:
                return new GetAllLocksAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteLock:
                return new DeleteLockAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case bulkDeleteLocks:
                return new BulkDeleteLocksAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case deleteAllLocks:
                return new DeleteAllLocksAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesSpeed:
                return new GetQueuesSpeedAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
                        consumersPrefix, locksKey, queueConfigurations, queueStatisticsCollector, log);
            case getQueuesStatistics:
                return new GetQueuesStatisticsAction(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix,
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
