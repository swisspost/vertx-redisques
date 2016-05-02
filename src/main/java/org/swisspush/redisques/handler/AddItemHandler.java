package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.swisspush.redisques.RedisQues;

/**
 * Class AddItemHandler.
 *
 * @author baldim
 */
public class AddItemHandler implements Handler<AsyncResult<Long>> {
    private Message<JsonObject> event;

    public AddItemHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<Long> reply) {
        if(reply.succeeded()){
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.OK));
        } else {
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.ERROR));
        }
    }
}
