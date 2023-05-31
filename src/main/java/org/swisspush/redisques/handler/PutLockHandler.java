package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import static org.swisspush.redisques.util.RedisquesAPI.*;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

/**
 * Class PutLock.
 *
 * @author baldim
 */
public class PutLockHandler implements Handler<AsyncResult<Response>> {
    private final Message<JsonObject> event;

    public PutLockHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if(reply.succeeded()){
            event.reply(new JsonObject().put(STATUS, OK));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
