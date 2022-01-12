package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.HttpServerRequestUtil;
import org.swisspush.redisques.util.QueueHandlerUtil;
import org.swisspush.redisques.util.RedisquesAPI;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class GetQueueItemsCountHandler.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class GetQueuesItemsCountHandler implements Handler<AsyncResult<JsonArray>> {

    private final Logger log = LoggerFactory.getLogger(GetQueuesItemsCountHandler.class);

    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final LuaScriptManager luaScriptManager;
    private final String queuesPrefix;

    public GetQueuesItemsCountHandler(Message<JsonObject> event, Optional<Pattern> filterPattern, LuaScriptManager luaScriptManager, String queuesPrefix) {
        this.event = event;
        this.filterPattern = filterPattern;
        this.luaScriptManager = luaScriptManager;
        this.queuesPrefix = queuesPrefix;
    }

    @Override
    public void handle(AsyncResult<JsonArray> handleQueues) {
        if(handleQueues.succeeded()){
            List<String> queues = QueueHandlerUtil.filterQueues(handleQueues.result(), filterPattern);
            if (queues==null || queues.isEmpty()) {
                log.debug("Queue count evaluation with empty queues");
                event.reply(new JsonObject().put(STATUS, OK).put(QUEUES, new JsonArray()));
                return;
            }
            List<String> keys = new ArrayList<>();
            for (String queue : queues) {
                keys.add(queuesPrefix + queue);
            }
            luaScriptManager.handleMultiListLength(keys, multiListLength -> {
                if (multiListLength==null) {
                    log.error("Unexepected queue MultiListLength result null");
                    event.reply(new JsonObject().put(STATUS, ERROR));
                    return;
                }
                if (multiListLength.size()!=queues.size()) {
                    log.error("Unexpected queue MultiListLength result with unequal size {} : {}", queues.size(), multiListLength.size());
                    event.reply(new JsonObject().put(STATUS, ERROR));
                    return;
                }
                JsonArray result = new JsonArray();
                for (int i = 0; i < queues.size(); i++) {
                    String queueName = queues.get(i);
                    result.add(new JsonObject()
                        .put("name", queueName)
                        .put("size", multiListLength.get(i)));
                }
                event.reply(new JsonObject().put(RedisquesAPI.STATUS, RedisquesAPI.OK)
                    .put(QUEUES, result));
            });
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }

}
