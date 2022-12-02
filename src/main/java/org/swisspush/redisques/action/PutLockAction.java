package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.swisspush.redisques.handler.PutLockHandler;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class PutLockAction extends LockRelatedQueueAction {

    public PutLockAction(RedisAPI redisAPI, String locksKey) {
        super(redisAPI, locksKey);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo != null) {
            JsonArray lockNames = new JsonArray().add(event.body().getJsonObject(PAYLOAD).getString(QUEUENAME));
            if (!jsonArrayContainsStringsOnly(lockNames)) {
                event.reply(QueueAction.createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Lock must be a string value"));
                return;
            }
            redisAPI.hmset(buildLocksItems(locksKey, lockNames, lockInfo), new PutLockHandler(event));
        } else {
            event.reply(QueueAction.createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
        }
    }
}
