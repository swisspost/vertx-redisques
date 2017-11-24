package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import static org.swisspush.redisques.util.RedisquesAPI.*;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Class GetQueueItemHandler.
 *
 * @author baldim, https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueueItemHandler implements Handler<AsyncResult<String>> {
    private Message<JsonObject> event;

    public GetQueueItemHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<String> reply) {
        if (reply.succeeded() && reply.result() != null) {
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, reply.result()));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
