package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.AddQueueItemHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class AddQueueItemAction extends AbstractQueueAction {

    public AddQueueItemAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper, queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        String key = keyspaceHelper.getQueuesPrefix() + queueName;
        String valueAddItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        redisService.rpush(key, valueAddItem).onComplete(rpushResult -> {
            if (rpushResult.succeeded()) {
                processTrimRequestByState(queueName).onComplete(asyncResult -> {
                    if (asyncResult.failed()) {
                        log.warn("Failed to do the trim for  {}", queueName);
                    }
                    new AddQueueItemHandler(event, exceptionFactory).handle(rpushResult);
                });
            } else {
                handleFail(event, "Operation AddQueueItemAction failed", rpushResult.cause());
            }
        });

    }

}
