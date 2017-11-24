package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class GetLockHandler.
 *
 * @author baldim
 */
public class GetLockHandler implements Handler<AsyncResult<String>> {
    private Message<JsonObject> event;

    public GetLockHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<String> reply) {
        if (reply.succeeded()) {
            if (reply.result() != null) {
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, reply.result()));
            } else {
                event.reply(new JsonObject().put(STATUS, NO_SUCH_LOCK));
            }
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
