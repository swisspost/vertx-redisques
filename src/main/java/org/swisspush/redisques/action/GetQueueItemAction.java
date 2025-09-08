package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetQueueItemHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class GetQueueItemAction extends AbstractQueueAction {


    public GetQueueItemAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
            List<QueueConfiguration> queueConfigurations, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String key = keyspaceHelper.getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int index = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);

        redisService.lindex(key, String.valueOf(index)).onComplete(response -> new GetQueueItemHandler(event, exceptionFactory).handle(response));
    }
}
