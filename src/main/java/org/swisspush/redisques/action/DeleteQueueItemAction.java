package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class DeleteQueueItemAction extends AbstractQueueAction {

    public DeleteQueueItemAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                                     String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                     QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String keyLset = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexLset = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        redisAPI.lset(keyLset, String.valueOf(indexLset), "TO_DELETE", event1 -> {
            if (event1.succeeded()) {
                String keyLrem = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
                redisAPI.lrem(keyLrem, "0", "TO_DELETE", replyLrem -> {
                    if (replyLrem.failed()) {
                        log.warn("Redis 'lrem' command failed. But will continue anyway.", replyLrem.cause());
                        // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                    }
                    event.reply(QueueAction.createOkReply());
                });
            } else {
                log.error("Failed to 'lset' while deleteQueueItem.", event1.cause());
                event.reply(QueueAction.createErrorReply());
            }
        });
    }
}
