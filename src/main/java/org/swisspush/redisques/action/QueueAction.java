package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public interface QueueAction {
    void execute(Message<JsonObject> event);
}
