package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.DeleteLockHandler;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

public class DeleteLockAction extends AbstractQueueAction {

    public DeleteLockAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
                            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                            QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        redisProvider.redis().onSuccess(redisAPI ->
                        redisAPI.exists(Collections.singletonList(queuesPrefix + queueName), event1 -> {
                            if (event1.succeeded() && event1.result() != null && event1.result().toInteger() == 1) {
                                notifyConsumer(queueName);
                            }
                            redisAPI.hdel(Arrays.asList(locksKey, queueName), new DeleteLockHandler(event));
                        }))
                .onFailure(replyErrorMessageHandler(event));
    }
}
