package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.redis.client.Response;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.HandlerUtil;
import org.swisspush.redisques.util.RedisquesAPI;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class GetQueuesItemsCountHandler implements Handler<AsyncResult<Response>> {

    private final Logger log = LoggerFactory.getLogger(GetQueuesItemsCountHandler.class);

    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final LuaScriptManager luaScriptManager;
    private final String queuesPrefix;

    public GetQueuesItemsCountHandler(
            Message<JsonObject> event,
            Optional<Pattern> filterPattern,
            LuaScriptManager luaScriptManager,
            String queuesPrefix) {
        this.event = event;
        this.filterPattern = filterPattern;
        this.luaScriptManager = luaScriptManager;
        this.queuesPrefix = queuesPrefix;
    }

    @Override
    public void handle(AsyncResult<Response> handleQueues) {
        if (handleQueues.succeeded()) {
            List<String> queues = HandlerUtil.filterByPattern(handleQueues.result(),
                filterPattern);
            if (queues.isEmpty()) {
                log.debug("Queue count evaluation with empty queues");
                event.reply(new JsonObject().put(STATUS, OK).put(QUEUES, new JsonArray()));
                return;
            }
            List<String> keys = queues.stream().map(queue -> queuesPrefix + queue)
                .collect(Collectors.toList());
            luaScriptManager.handleMultiListLength(keys, multiListLength -> {
                if (multiListLength==null) {
                    log.error("Unexepected queue MultiListLength result null");
                    event.reply(new JsonObject().put(STATUS, ERROR));
                    return;
                }
                if (multiListLength.size()!=queues.size()) {
                    log.error("Unexpected queue MultiListLength result with unequal size {} : {}",
                        queues.size(), multiListLength.size());
                    event.reply(new JsonObject().put(STATUS, ERROR));
                    return;
                }
                JsonArray result = new JsonArray();
                for (int i = 0; i < queues.size(); i++) {
                    String queueName = queues.get(i);
                    result.add(new JsonObject()
                        .put(MONITOR_QUEUE_NAME, queueName)
                        .put(MONITOR_QUEUE_SIZE, multiListLength.get(i)));
                }
                event.reply(new JsonObject().put(RedisquesAPI.STATUS, RedisquesAPI.OK)
                    .put(QUEUES, result));
            });
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }

}
