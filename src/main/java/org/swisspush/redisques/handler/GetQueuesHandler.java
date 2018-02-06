package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class GetQueuesCountHandler.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueuesHandler implements Handler<AsyncResult<JsonArray>> {

    private Message<JsonObject> event;
    private Optional<Pattern> filterPattern;
    private boolean countOnly;

    private static final String QUEUES_ARR = "queues";

    public GetQueuesHandler(Message<JsonObject> event, Optional<Pattern> filterPattern, boolean countOnly) {
        this.event = event;
        this.filterPattern = filterPattern;
        this.countOnly = countOnly;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if(reply.succeeded()){
            JsonObject result = new JsonObject();
            JsonArray queues = reply.result();
            if(filterPattern.isPresent()){
                Pattern pattern = filterPattern.get();
                JsonArray filteredQueues = new JsonArray();
                for (int i = 0; i < queues.size(); i++) {
                    String queue = queues.getString(i);
                    if(pattern.matcher(queue).find()){
                        filteredQueues.add(queue);
                    }
                }
                result.put(QUEUES_ARR, filteredQueues);
            } else {
                result.put(QUEUES_ARR, queues);
            }
            if(countOnly){
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result.getJsonArray(QUEUES_ARR).size()));
            } else {
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
            }

        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
