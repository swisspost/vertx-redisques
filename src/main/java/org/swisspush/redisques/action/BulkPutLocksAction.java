package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.PutLockHandler;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;
import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;

public class BulkPutLocksAction extends LockRelatedQueueAction {


    public BulkPutLocksAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                                  String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                  QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonArray locks = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (locks == null || locks.isEmpty()) {
            event.reply(QueueAction.createErrorReply().put(MESSAGE, "No locks to put provided"));
            return;
        }

        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo == null) {
            event.reply(QueueAction.createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
            return;
        }

        if (!jsonArrayContainsStringsOnly(locks)) {
            event.reply(QueueAction.createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Locks must be string values"));
            return;
        }

        redisAPI.hmset(buildLocksItems(locksKey, locks, lockInfo), new PutLockHandler(event));
    }
}
