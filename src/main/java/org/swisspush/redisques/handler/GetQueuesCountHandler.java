package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;
import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

/**
 * Class GetQueuesCountHandler.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueuesCountHandler implements Handler<AsyncResult<Response>> {

    private final Message<JsonObject> event;
    private final RedisQuesExceptionFactory exceptionFactory;

    public GetQueuesCountHandler(Message<JsonObject> event, RedisQuesExceptionFactory exceptionFactory) {
        this.event = event;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if(reply.succeeded()){
            Long queueCount = reply.result().toLong();
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, queueCount));
        } else {
            // For whatever reason 'event' cannot transport a regular exception. So
            // we have to squeeze it into a ReplyException.
            ReplyException replyEx = exceptionFactory.newReplyException(RECIPIENT_FAILURE, 500, ERROR);
            replyEx.initCause(reply.cause());
            event.reply(replyEx);
        }
    }

}
