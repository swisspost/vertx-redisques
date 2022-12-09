package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public interface QueueAction {
    void execute(Message<JsonObject> event);

    default JsonObject createOkReply() {
        return new JsonObject().put(STATUS, OK);
    }

    default JsonObject createErrorReply() {
        return new JsonObject().put(STATUS, ERROR);
    }
}
