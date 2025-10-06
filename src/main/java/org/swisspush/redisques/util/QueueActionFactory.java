package org.swisspush.redisques.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import org.slf4j.Logger;
import org.swisspush.redisques.action.AddQueueItemAction;
import org.swisspush.redisques.action.BulkDeleteLocksAction;
import org.swisspush.redisques.action.BulkDeleteQueuesAction;
import org.swisspush.redisques.action.BulkPutLocksAction;
import org.swisspush.redisques.action.DeleteAllLocksAction;
import org.swisspush.redisques.action.DeleteAllQueueItemsAction;
import org.swisspush.redisques.action.DeleteLockAction;
import org.swisspush.redisques.action.DeleteQueueItemAction;
import org.swisspush.redisques.action.EnqueueAction;
import org.swisspush.redisques.action.GetAllLocksAction;
import org.swisspush.redisques.action.GetConfigurationAction;
import org.swisspush.redisques.action.GetLockAction;
import org.swisspush.redisques.action.GetQueueItemAction;
import org.swisspush.redisques.action.GetQueueItemsAction;
import org.swisspush.redisques.action.GetQueueItemsCountAction;
import org.swisspush.redisques.action.GetQueuesAction;
import org.swisspush.redisques.action.GetQueuesCountAction;
import org.swisspush.redisques.action.GetQueuesItemsCountAction;
import org.swisspush.redisques.action.GetQueuesSpeedAction;
import org.swisspush.redisques.action.GetQueuesStatisticsAction;
import org.swisspush.redisques.action.LockedEnqueueAction;
import org.swisspush.redisques.action.MonitorAction;
import org.swisspush.redisques.action.PutLockAction;
import org.swisspush.redisques.action.QueueAction;
import org.swisspush.redisques.action.ReplaceQueueItemAction;
import org.swisspush.redisques.action.SetConfigurationAction;
import org.swisspush.redisques.action.UnsupportedAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;

import java.util.List;
import java.util.concurrent.Semaphore;

public class QueueActionFactory {

    private final RedisService redisService;
    private final Vertx vertx;
    private final HttpClient client;
    private final Logger log;
    private final List<QueueConfiguration> queueConfigurations;
    private final QueueStatisticsCollector queueStatisticsCollector;
    private final int memoryUsageLimitPercent;
    private final MeterRegistry meterRegistry;
    private final String metricsIdentifier;
    private final MemoryUsageProvider memoryUsageProvider;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final Semaphore getQueuesItemsCountRedisRequestQuota;

    private final RedisquesConfigurationProvider configurationProvider;
    private final KeyspaceHelper keyspaceHelper;

    public QueueActionFactory(
        RedisService redisService,
        Vertx vertx,
        HttpClient client,
        Logger log,
        KeyspaceHelper keyspaceHelper,
        MemoryUsageProvider memoryUsageProvider,
        QueueStatisticsCollector queueStatisticsCollector,
        RedisQuesExceptionFactory exceptionFactory,
        RedisquesConfigurationProvider configurationProvider,
        Semaphore getQueuesItemsCountRedisRequestQuota,
        MeterRegistry meterRegistry
    ) {
        this.redisService = redisService;
        this.vertx = vertx;
        this.client = client;
        this.log = log;
        this.keyspaceHelper = keyspaceHelper;
        this.memoryUsageProvider = memoryUsageProvider;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.exceptionFactory = exceptionFactory;
        this.configurationProvider = configurationProvider;
        this.queueConfigurations = configurationProvider.configuration().getQueueConfigurations();
        this.memoryUsageLimitPercent = configurationProvider.configuration().getMemoryUsageLimitPercent();
        this.getQueuesItemsCountRedisRequestQuota = getQueuesItemsCountRedisRequestQuota;
        this.meterRegistry = meterRegistry;

        metricsIdentifier = configurationProvider.configuration().getMicrometerMetricsIdentifier();
    }

    public QueueAction buildQueueAction(RedisquesAPI.QueueOperation queueOperation){
        switch (queueOperation){
            case addQueueItem:
                return new AddQueueItemAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteQueueItem:
                return new DeleteQueueItemAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteAllQueueItems:
                return new DeleteAllQueueItemsAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case bulkDeleteQueues:
                return new BulkDeleteQueuesAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case replaceQueueItem:
                return new ReplaceQueueItemAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueueItem:
                return new GetQueueItemAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueueItems:
                return new GetQueueItemsAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueues:
                return new GetQueuesAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesCount:
                return new GetQueuesCountAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueueItemsCount:
                return new GetQueueItemsCountAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesItemsCount:
                return new GetQueuesItemsCountAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory,
                        getQueuesItemsCountRedisRequestQuota, queueStatisticsCollector, log);
            case enqueue:
                return new EnqueueAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log,
                        memoryUsageProvider, memoryUsageLimitPercent, meterRegistry, metricsIdentifier);
            case lockedEnqueue:
                return new LockedEnqueueAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log,
                        memoryUsageProvider, memoryUsageLimitPercent, meterRegistry, metricsIdentifier);
            case getLock:
                return new GetLockAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case putLock:
                return new PutLockAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case bulkPutLocks:
                return new BulkPutLocksAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getAllLocks:
                return new GetAllLocksAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteLock:
                return new DeleteLockAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case bulkDeleteLocks:
                return new BulkDeleteLocksAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case deleteAllLocks:
                return new DeleteAllLocksAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesSpeed:
                return new GetQueuesSpeedAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case getQueuesStatistics:
                return new GetQueuesStatisticsAction(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
            case setConfiguration:
                return new SetConfigurationAction(configurationProvider, log);
            case getConfiguration:
                return new GetConfigurationAction(configurationProvider);
            case monitor:
                return new MonitorAction(configurationProvider.configuration(), client, log);
            default:
                return new UnsupportedAction(log);
        }
    }
    
    public QueueAction buildUnsupportedAction() {
        return new UnsupportedAction(log);
    }
}
