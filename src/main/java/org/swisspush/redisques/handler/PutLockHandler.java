package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import org.swisspush.redisques.RedisQues;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Class PutLock.
 *
 * @author baldim
 */
public class PutLockHandler implements Handler<AsyncResult<String>> {
    private Message<JsonObject> event;

    public PutLockHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<String> reply) {
        if(reply.succeeded()){
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.OK));
        } else {
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.ERROR));
        }
    }
}
