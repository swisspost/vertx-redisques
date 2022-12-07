package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.MultiType;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.slf4j.Logger;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class BulkDeleteLocksAction extends AbstractQueueAction {

    public BulkDeleteLocksAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                                  String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                  QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonArray jsonArray = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (jsonArray != null) {
            MultiType locks = MultiType.create(jsonArray.size(), false);
            for (int j = 0; j < jsonArray.size(); j++) {
                Response response = SimpleStringType.create(jsonArray.getString(j));
                locks.add(response);
            }
            deleteLocks(event, locks);
        } else {
            event.reply(createErrorReply().put(MESSAGE, "No locks to delete provided"));
        }
    }
}
