package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonArray;
import org.swisspush.redisques.RedisQues;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Class GetListRangeHandler.
 *
 * @author baldim, webermarca
 */
public class GetListRangeHandler implements Handler<AsyncResult<JsonArray>> {
    private Message<JsonObject> event;
    private Long queueItemCount;

    public GetListRangeHandler(Message<JsonObject> event, Long queueItemCount) {
        this.event = event;
        this.queueItemCount = queueItemCount;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if(reply.succeeded()){
            JsonArray resultArray = reply.result();
            JsonArray countInfo = new JsonArray();
            countInfo.add(resultArray.size());
            countInfo.add(queueItemCount);
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.OK).put(RedisQues.VALUE, resultArray).put(RedisQues.INFO, countInfo));
        } else {
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.ERROR));
        }
    }
}
