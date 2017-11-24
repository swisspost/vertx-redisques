package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class GetQueuesCountHandler.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueuesHandler implements Handler<AsyncResult<JsonArray>> {
    private Message<JsonObject> event;

    public GetQueuesHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if(reply.succeeded()){
            JsonObject result = new JsonObject();
            result.put("queues", reply.result());
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
