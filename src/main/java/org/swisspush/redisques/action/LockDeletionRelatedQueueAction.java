package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

public abstract class LockDeletionRelatedQueueAction extends LockRelatedQueueAction {

    public LockDeletionRelatedQueueAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                                  String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                  QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    protected void deleteLocks(Message<JsonObject> event, Response locks) {
        if (locks == null || locks.size() == 0) {
            event.reply(QueueAction.createOkReply().put(VALUE, 0));
            return;
        }

        List<String> args = new ArrayList<>();
        args.add(locksKey);
        for (Response response : locks) {
            args.add(response.toString());
        }

        redisAPI.hdel(args, delManyResult -> {
            if (delManyResult.succeeded()) {
                log.info("Successfully deleted " + delManyResult.result() + " locks");
                event.reply(QueueAction.createOkReply().put(VALUE, delManyResult.result().toLong()));
            } else {
                log.warn("failed to delete locks. Message: " + delManyResult.cause().getMessage());
                event.reply(QueueAction.createErrorReply().put(MESSAGE, delManyResult.cause().getMessage()));
            }
        });
    }
}
