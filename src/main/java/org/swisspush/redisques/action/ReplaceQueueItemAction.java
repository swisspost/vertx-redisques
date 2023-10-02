package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.ReplaceQueueItemHandler;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisUtils;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class ReplaceQueueItemAction extends AbstractQueueAction {

    public ReplaceQueueItemAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
                                  String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                  QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String keyReplaceItem = queuesPrefix + RedisUtils.formatAsHastag(event.body().getJsonObject(PAYLOAD).getString(QUEUENAME));
        int indexReplaceItem = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        String bufferReplaceItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.lset(keyReplaceItem,
                        String.valueOf(indexReplaceItem), bufferReplaceItem, new ReplaceQueueItemHandler(event)))
                .onFailure(replyErrorMessageHandler(event));
    }
}
