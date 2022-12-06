package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class BulkDeleteQueuesAction extends AbstractQueueAction {

    public BulkDeleteQueuesAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                                     String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                     QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonArray queues = event.body().getJsonObject(PAYLOAD).getJsonArray(QUEUES);
        if (queues == null) {
            event.reply(createErrorReply().put(MESSAGE, "No queues to delete provided"));
            return;
        }

        if (queues.isEmpty()) {
            event.reply(createOkReply().put(VALUE, 0));
            return;
        }

        if (!jsonArrayContainsStringsOnly(queues)) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Queues must be string values"));
            return;
        }

        redisAPI.del(buildQueueKeys(queues), delManyReply -> {
            queueStatisticsCollector.resetQueueStatistics(queues);
            if (delManyReply.succeeded()) {
                event.reply(createOkReply().put(VALUE, delManyReply.result().toLong()));
            } else {
                log.error("Failed to bulkDeleteQueues", delManyReply.cause());
                event.reply(createErrorReply());
            }
        });
    }
}
