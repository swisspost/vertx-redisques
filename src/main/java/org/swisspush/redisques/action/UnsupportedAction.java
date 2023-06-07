package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class UnsupportedAction implements QueueAction {

    protected final Logger log;

    public UnsupportedAction(Logger log) {
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        final JsonObject body = event.body();
        String message;
        if (null != body) {
            message = "QUEUE_ERROR: Unsupported operation received: " + body.getString(OPERATION);
        } else {
            message = "QUEUE_ERROR: Unsupported operation received";
        }

        JsonObject reply = new JsonObject();
        log.error(message);
        reply.put(STATUS, ERROR);
        reply.put(MESSAGE, message);
        event.reply(reply);
    }
}
