package org.swisspush.redisques.action;


import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetLockHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

public class GetLockAction extends AbstractQueueAction {

    public GetLockAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
            List<QueueConfiguration> queueConfigurations, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        final JsonObject body = event.body();
        if (body == null) {
            replyErrorMessageHandler(event).handle(new NullPointerException("Got msg with no body from event bus. address=" +
                    event.address() + " replyAddress=" + event.replyAddress()));
            return;
        }
        redisService.hget(keyspaceHelper.getLocksKey(), body.getJsonObject(PAYLOAD).getString(QUEUENAME)).onComplete(response -> new GetLockHandler(event, exceptionFactory).handle(response));
    }
}
