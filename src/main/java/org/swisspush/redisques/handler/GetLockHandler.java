package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.NO_SUCH_LOCK;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

/**
 * Class GetLockHandler.
 *
 * @author baldim
 */
public class GetLockHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(GetLockHandler.class);
    private final Message<JsonObject> event;
    private final RedisQuesExceptionFactory exceptionFactory;

    public GetLockHandler(Message<JsonObject> event, RedisQuesExceptionFactory exceptionFactory) {
        this.event = event;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if (reply.succeeded()) {
            if (reply.result() != null) {
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, reply.result().toString()));
            } else {
                event.reply(new JsonObject().put(STATUS, NO_SUCH_LOCK));
            }
        } else {
            log.warn("Concealed error", exceptionFactory.newException(reply.cause()));
            event.fail(0, reply.cause().getMessage());
        }
    }

}
