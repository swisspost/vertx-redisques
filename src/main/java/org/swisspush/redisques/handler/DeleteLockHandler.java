package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class DeleteLockHandler.
 *
 * @author baldim
 */
public class DeleteLockHandler implements Handler<AsyncResult<Response>> {
    private Message<JsonObject> event;

    public DeleteLockHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if (reply.succeeded()) {
            event.reply(new JsonObject().put(STATUS, OK));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
