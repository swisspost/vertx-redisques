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
 * Class GetAllLocksHandler.
 *
 * @author baldim
 */
public class GetAllLocksHandler implements Handler<AsyncResult<JsonArray>> {

    private Message<JsonObject> event;
    private Optional<Pattern> filterPattern;

    public GetAllLocksHandler(Message<JsonObject> event, Optional<Pattern> filterPattern) {
        this.event = event;
        this.filterPattern = filterPattern;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if (reply.succeeded() && reply.result() != null) {
            JsonObject result = new JsonObject();
            JsonArray locks = reply.result();
            if(filterPattern.isPresent()){
                Pattern pattern = filterPattern.get();
                JsonArray filteredLocks = new JsonArray();
                for (int i = 0; i < locks.size(); i++) {
                    String lock = locks.getString(i);
                    if(pattern.matcher(lock).find()){
                        filteredLocks.add(lock);
                    }
                }
                result.put("locks", filteredLocks);
            } else {
                result.put("locks", locks);
            }
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}