package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonArray;

import static org.swisspush.redisques.util.RedisquesAPI.*;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

/**
 * Class GetQueueItemsHandler.
 *
 * @author baldim, https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueueItemsHandler implements Handler<AsyncResult<Response>> {
    private Message<JsonObject> event;
    private Long queueItemCount;

    public GetQueueItemsHandler(Message<JsonObject> event, Long queueItemCount) {
        this.event = event;
        this.queueItemCount = queueItemCount;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if (reply.succeeded()) {
            Response result = reply.result();
            JsonArray countInfo = new JsonArray();
            if (result != null) {
                countInfo.add(result.size());
            }
            countInfo.add(queueItemCount);
            JsonArray values = new JsonArray();
            for (Response res : result) {
                values.add(res.toString());
            }
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, values).put(INFO, countInfo));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
