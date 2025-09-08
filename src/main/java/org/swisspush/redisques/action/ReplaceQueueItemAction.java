package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.ReplaceQueueItemHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class ReplaceQueueItemAction extends AbstractQueueAction {

    public ReplaceQueueItemAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
            List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String keyReplaceItem = keyspaceHelper.getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexReplaceItem = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        String bufferReplaceItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        redisService.lset(keyReplaceItem, String.valueOf(indexReplaceItem),
                bufferReplaceItem).onComplete(response -> new ReplaceQueueItemHandler(event, exceptionFactory).handle(response));
    }

}
