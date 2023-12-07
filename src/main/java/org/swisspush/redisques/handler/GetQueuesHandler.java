package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.util.HandlerUtil;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUES;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

/**
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueuesHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(GetQueuesHandler.class);
    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final boolean countOnly;

    public GetQueuesHandler(Message<JsonObject> event, Optional<Pattern> filterPattern, boolean countOnly) {
        this.event = event;
        this.filterPattern = filterPattern;
        this.countOnly = countOnly;
    }

    @Override
    public void handle(AsyncResult<Response> reply) {
        if(reply.succeeded()){
            JsonObject jsonRes = new JsonObject();
            Response queues = reply.result();
            List<String> filteredQueues = HandlerUtil.filterByPattern(queues, filterPattern);
            jsonRes.put(QUEUES, new JsonArray(filteredQueues));
            if(countOnly){
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, jsonRes.getJsonArray(QUEUES).size()));
            } else {
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, jsonRes));
            }
        } else {
            log.warn("Concealed error", new Exception(reply.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
