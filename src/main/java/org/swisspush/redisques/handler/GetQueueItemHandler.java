package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

/**
 * Class GetQueueItemHandler.
 *
 * @author baldim, <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class GetQueueItemHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(GetQueueItemHandler.class);
    private final Message<JsonObject> event;
    private final RedisQuesExceptionFactory exceptionFactory;

    public GetQueueItemHandler(Message<JsonObject> event, RedisQuesExceptionFactory exceptionFactory) {
        this.event = event;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if(reply.failed()) {
            event.reply(exceptionFactory.newReplyException(null, reply.cause()));
        } else if (reply.result() != null) {
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, reply.result().toString()));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }

}
