package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

/**
 * Class DeleteLockHandler.
 *
 * @author baldim
 */
public class DeleteLockHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(DeleteLockHandler.class);
    private final Message<JsonObject> event;

    public DeleteLockHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if (reply.succeeded()) {
            event.reply(new JsonObject().put(STATUS, OK));
        } else {
            log.warn("Concealed error", new Exception(reply.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
