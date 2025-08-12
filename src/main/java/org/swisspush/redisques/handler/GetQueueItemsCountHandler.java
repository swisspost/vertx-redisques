package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

/**
 * Class GetQueueItemsCountHandler.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class GetQueueItemsCountHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(GetQueueItemsCountHandler.class);
    private final Message<JsonObject> event;
    private final QueueConfiguration queueConfiguration;

    public GetQueueItemsCountHandler(Message<JsonObject> event, QueueConfiguration queueConfiguration) {
        this.event = event;
        this.queueConfiguration = queueConfiguration;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if (reply.succeeded()) {
            Long queueItemCount = reply.result().toLong();
            if (queueConfiguration != null && queueConfiguration.getMaxQueueEntries() > 0) {
                final long maxQueueEntries = queueConfiguration.getMaxQueueEntries();
                queueItemCount = Math.min(queueItemCount, maxQueueEntries);
            }
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, queueItemCount));
        } else {
            log.warn("Concealed error", new Exception(reply.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
