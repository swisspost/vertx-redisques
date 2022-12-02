package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.Arrays;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;
import static org.swisspush.redisques.util.RedisquesAPI.REQUESTED_BY;

public class LockedEnqueueAction extends EnqueueAction {

    private String locksKey;

    public LockedEnqueueAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                                     String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                     QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        log.debug("RedisQues about to lockedEnqueue");
        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo != null) {
            redisAPI.hmset(Arrays.asList(locksKey, event.body().getJsonObject(PAYLOAD).getString(QUEUENAME), lockInfo.encode()),
                    putLockResult -> {
                        if (putLockResult.succeeded()) {
                            log.debug("RedisQues lockedEnqueue locking successful, now going to enqueue");
                            super.execute(event);
                        } else {
                            log.warn("RedisQues lockedEnqueue locking failed. Skip enqueue");
                            event.reply(QueueAction.createErrorReply());
                        }
                    });
        } else {
            log.warn("RedisQues lockedEnqueue failed because property '" + REQUESTED_BY + "' was missing");
            event.reply(QueueAction.createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
        }
    }
}
