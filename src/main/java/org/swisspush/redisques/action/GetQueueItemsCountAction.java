package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetQueueItemsCountHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;

import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

/**
 * Retrieve the number of items of the requested queue
 */
public class GetQueueItemsCountAction extends AbstractQueueAction {

    public GetQueueItemsCountAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
            QueueConfigurationProvider queueConfigurationProvider,
            RedisquesConfigurationProvider redisquesConfigurationProvider, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurationProvider, redisquesConfigurationProvider, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queue = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        redisService.llen(keyspaceHelper.getQueuesPrefix() + queue).onComplete(response -> new GetQueueItemsCountHandler(event).handle(response));
    }

}
