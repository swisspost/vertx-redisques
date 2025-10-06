package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.action.QueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.MemoryUsageProvider;
import org.swisspush.redisques.util.QueueActionFactory;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesAPI;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.addQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.bulkDeleteLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.bulkDeleteQueues;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.bulkPutLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteAllLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteAllQueueItems;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteLock;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.deleteQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.enqueue;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getAllLocks;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getConfiguration;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getLock;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueueItems;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueueItemsCount;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueues;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesCount;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesItemsCount;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesSpeed;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.getQueuesStatistics;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.lockedEnqueue;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.monitor;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.putLock;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.replaceQueueItem;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.setConfiguration;

public class QueueActionsService {
    private static final Logger log = LoggerFactory.getLogger(QueueActionsService.class);
    private final QueueActionFactory queueActionFactory;
    private final Map<RedisquesAPI.QueueOperation, QueueAction> queueActions = new HashMap<>();
    private final RedisQuesExceptionFactory exceptionFactory;

    public QueueActionsService(Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
                               RedisquesConfigurationProvider configurationProvider,
                               RedisQuesExceptionFactory exceptionFactory, MemoryUsageProvider memoryUsageProvider,
                               QueueStatisticsCollector queueStatisticsCollector,
                               Semaphore getQueuesItemsCountRedisRequestQuota,
                               MeterRegistry meterRegistry) {
        this.exceptionFactory = exceptionFactory;
        HttpClient client = vertx.createHttpClient();

        this.queueActionFactory = new QueueActionFactory(
                redisService, vertx, client, log, keyspaceHelper,
                memoryUsageProvider, queueStatisticsCollector, exceptionFactory,
                configurationProvider, getQueuesItemsCountRedisRequestQuota, meterRegistry);

        queueActions.put(addQueueItem, queueActionFactory.buildQueueAction(addQueueItem));
        queueActions.put(deleteQueueItem, queueActionFactory.buildQueueAction(deleteQueueItem));
        queueActions.put(deleteAllQueueItems, queueActionFactory.buildQueueAction(deleteAllQueueItems));
        queueActions.put(bulkDeleteQueues, queueActionFactory.buildQueueAction(bulkDeleteQueues));
        queueActions.put(replaceQueueItem, queueActionFactory.buildQueueAction(replaceQueueItem));
        queueActions.put(getQueueItem, queueActionFactory.buildQueueAction(getQueueItem));
        queueActions.put(getQueueItems, queueActionFactory.buildQueueAction(getQueueItems));
        queueActions.put(getQueues, queueActionFactory.buildQueueAction(getQueues));
        queueActions.put(getQueuesCount, queueActionFactory.buildQueueAction(getQueuesCount));
        queueActions.put(getQueueItemsCount, queueActionFactory.buildQueueAction(getQueueItemsCount));
        queueActions.put(getQueuesItemsCount, queueActionFactory.buildQueueAction(getQueuesItemsCount));
        queueActions.put(enqueue, queueActionFactory.buildQueueAction(enqueue));
        queueActions.put(lockedEnqueue, queueActionFactory.buildQueueAction(lockedEnqueue));
        queueActions.put(getLock, queueActionFactory.buildQueueAction(getLock));
        queueActions.put(putLock, queueActionFactory.buildQueueAction(putLock));
        queueActions.put(bulkPutLocks, queueActionFactory.buildQueueAction(bulkPutLocks));
        queueActions.put(getAllLocks, queueActionFactory.buildQueueAction(getAllLocks));
        queueActions.put(deleteLock, queueActionFactory.buildQueueAction(deleteLock));
        queueActions.put(bulkDeleteLocks, queueActionFactory.buildQueueAction(bulkDeleteLocks));
        queueActions.put(deleteAllLocks, queueActionFactory.buildQueueAction(deleteAllLocks));
        queueActions.put(getQueuesSpeed, queueActionFactory.buildQueueAction(getQueuesSpeed));
        queueActions.put(getQueuesStatistics, queueActionFactory.buildQueueAction(getQueuesStatistics));
        queueActions.put(setConfiguration, queueActionFactory.buildQueueAction(setConfiguration));
        queueActions.put(getConfiguration, queueActionFactory.buildQueueAction(getConfiguration));
        queueActions.put(monitor, queueActionFactory.buildQueueAction(monitor));

    }

    public void handle(RedisquesAPI.QueueOperation queueOperation, Message<JsonObject> event) {
        // handle queue operations
        QueueAction action = queueActions.getOrDefault(queueOperation, queueActionFactory.buildUnsupportedAction());
        action.execute(event);
    }
}
