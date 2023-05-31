package org.swisspush.redisques.action;


import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.GetLockHandler;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisAPIProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

public class GetLockAction extends AbstractQueueAction {

    public GetLockAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPIProvider redisAPIProvider, String address, String queuesKey, String queuesPrefix,
                         String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                         QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPIProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }


    @Override
    public void execute(Message<JsonObject> event) {
        final JsonObject body = event.body();
        if (null == body) {
            log.warn("Got msg with empty body from event bus. We'll run directly in " +
                    "a NullPointerException now. address={}  replyAddress={} ", event.address(), event.replyAddress());
            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
        }
        redisAPIProvider.redisAPI().onSuccess(redisAPI ->
                redisAPI.hget(locksKey, body.getJsonObject(PAYLOAD).getString(QUEUENAME), new GetLockHandler(event)));
    }

}
