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
            List<String> filteredLocks = QueueHandlerUtil.filterQueues(reply.result(), filterPattern);
            result.put(LOCKS, filteredLocks);
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}