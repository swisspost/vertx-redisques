package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import static org.swisspush.redisques.util.RedisquesAPI.*;
import static org.swisspush.redisques.util.RedisquesAPI.TIMESTAMP;

public interface QueueAction {
    void execute(Message<JsonObject> event);

    static JsonObject createOkReply() {
        return new JsonObject().put(STATUS, OK);
    }

    static JsonObject createErrorReply() {
        return new JsonObject().put(STATUS, ERROR);
    }

    default JsonObject extractLockInfo(String requestedBy) {
        if (requestedBy == null) {
            return null;
        }
        JsonObject lockInfo = new JsonObject();
        lockInfo.put(REQUESTED_BY, requestedBy);
        lockInfo.put(TIMESTAMP, System.currentTimeMillis());
        return lockInfo;
    }
}
