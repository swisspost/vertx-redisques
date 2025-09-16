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

/**
 * Class ReplaceQueueItemHandler.
 *
 * @author baldim, <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class ReplaceQueueItemHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(ReplaceQueueItemHandler.class);
    private final Message<JsonObject> event;
    private final RedisQuesExceptionFactory exceptionFactory;

    public ReplaceQueueItemHandler(Message<JsonObject> event, RedisQuesExceptionFactory exceptionFactory) {
        this.event = event;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if(reply.succeeded()){
            event.reply(new JsonObject().put(STATUS, OK));
        } else if(checkRedisErrorCodes(reply.cause().getMessage())) {
            event.reply(new JsonObject().put(STATUS, ERROR));
        } else {
            event.reply(exceptionFactory.newReplyException("Operation ReplaceQueueItemAction failed", reply.cause()));
        }
    }

    private boolean checkRedisErrorCodes(String message) {
        if(message == null) {
            return false;
        }
        return message.contains("index out of range");
    }
}
