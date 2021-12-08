package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class GetAllLocksHandler.
 *
 * @author baldim
 */
public class GetAllLocksHandler implements Handler<AsyncResult<Response>> {

    private Message<JsonObject> event;
    private Optional<Pattern> filterPattern;

    public GetAllLocksHandler(Message<JsonObject> event, Optional<Pattern> filterPattern) {
        this.event = event;
        this.filterPattern = filterPattern;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if (reply.succeeded() && reply.result() != null) {
            JsonObject result = new JsonObject();
            Response locks = reply.result();
            if (filterPattern.isPresent()) {
                Pattern pattern = filterPattern.get();
                JsonArray filteredLocks = new JsonArray();
                for (int i = 0; i < locks.size(); i++) {
                    String lock = locks.get(i).toString();
                    if (pattern.matcher(lock).find()) {
                        filteredLocks.add(lock);
                    }
                }
                result.put("locks", filteredLocks);
            } else {
                JsonArray values = new JsonArray();
                for (Response res : locks) {
                    values.add(res.toString());
                }
                result.put("locks", values);
            }
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}