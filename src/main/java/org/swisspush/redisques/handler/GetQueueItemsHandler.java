package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.INFO;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

/**
 * Class GetQueueItemsHandler.
 *
 * @author baldim, https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueueItemsHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(GetQueueItemsHandler.class);
    private final Message<JsonObject> event;
    private final Long queueItemCount;

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
            log.warn("Concealed error", new Exception(reply.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }

}
