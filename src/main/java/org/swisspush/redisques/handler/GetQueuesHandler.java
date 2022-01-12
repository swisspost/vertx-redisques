package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.swisspush.redisques.util.QueueHandlerUtil;
import org.swisspush.redisques.util.RedisquesAPI;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class GetQueuesHandler.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueuesHandler implements Handler<AsyncResult<JsonArray>> {

    private Message<JsonObject> event;
    private Optional<Pattern> filterPattern;
    private boolean countOnly;

    public GetQueuesHandler(Message<JsonObject> event, Optional<Pattern> filterPattern, boolean countOnly) {
        this.event = event;
        this.filterPattern = filterPattern;
        this.countOnly = countOnly;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if(reply.succeeded()){
            JsonObject result = new JsonObject();
            List<String> queues = QueueHandlerUtil.filterQueues(reply.result(), filterPattern);
            result.put(QUEUES, queues);
            if(countOnly){
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result.getJsonArray(QUEUES).size()));
            } else {
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
            }
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
