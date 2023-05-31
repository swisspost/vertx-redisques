package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisAPIProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;

public class DeleteAllLocksAction extends AbstractQueueAction {

    public DeleteAllLocksAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPIProvider redisAPIProvider, String address, String queuesKey, String queuesPrefix,
                                String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPIProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        redisAPIProvider.redisAPI().onSuccess(redisAPI -> redisAPI.hkeys(locksKey, locksResult -> {
            if (locksResult.succeeded()) {
                Response locks = locksResult.result();
                deleteLocks(event, locks);
            } else {
                log.warn("failed to delete all locks. Message: {}", locksResult.cause().getMessage());
                event.reply(createErrorReply().put(MESSAGE, locksResult.cause().getMessage()));
            }
        }));
    }
}
