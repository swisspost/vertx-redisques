package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.swisspush.redisques.RedisQues;

import java.util.List;

/**
 * Class GetAllLocksHandler.
 *
 * @author baldim
 */
public class GetAllLocksHandler implements Handler<AsyncResult<JsonArray>> {

    private Message<JsonObject> event;

    public GetAllLocksHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if(reply.succeeded()){
            List<Object> list = reply.result().getList();
            JsonObject result = new JsonObject();
            JsonArray items = new JsonArray();
            for (Object item : list.toArray()) {
                items.add((String) item);
            }
            result.put("locks", items);
            event.reply(new JsonObject()
                    .put(RedisQues.STATUS, RedisQues.OK)
                    .put(RedisQues.VALUE, result)
            );
        } else {
            event.reply(new JsonObject()
                    .put(RedisQues.STATUS, RedisQues.ERROR)
            );
        }
    }
}