package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import static org.swisspush.redisques.util.RedisquesAPI.*;

import java.util.List;

/**
 * Class GetAllLocksHandler.
 *
 * @author baldim
 */
public class GetAllLocksHandler implements Handler<AsyncResult<JsonArray>> {

    private Message<JsonObject> event;

    public GetAllLocksHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if (reply.succeeded() && reply.result() != null) {
            JsonObject result = new JsonObject();
            result.put("locks", reply.result());
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}