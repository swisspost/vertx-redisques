package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.DeleteLockHandler;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

public class DeleteLockAction extends AbstractQueueAction {

    public DeleteLockAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        var p = redisProvider.redis();
        p.onSuccess(redisAPI -> {
            redisAPI.exists(Collections.singletonList(queuesPrefix + queueName), event1 -> {
                if( event1.failed() ) log.warn("Concealed error", new Exception(event1.cause()));
                if (event1.succeeded() && event1.result() != null && event1.result().toInteger() == 1) {
                    notifyConsumer(queueName);
                }
                redisAPI.hdel(Arrays.asList(locksKey, queueName), new DeleteLockHandler(event));
            });
        });
        p.onFailure(ex -> replyErrorMessageHandler(event).handle(ex));
    }

}
