package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.GetQueuesHandler;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.MessageUtil;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.Result;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class GetQueuesAction extends AbstractQueueAction {

    public GetQueuesAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                           String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                           QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        getQueues(event, false, result);
    }

    protected void getQueues(Message<JsonObject> event, boolean countOnly, Result<Optional<Pattern>, String> filterPatternResult) {
        if (filterPatternResult.isErr()) {
            event.reply(QueueAction.createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, filterPatternResult.getErr()));
        } else {
            redisAPI.zrangebyscore(
                    Arrays.asList(queuesKey, String.valueOf(getMaxAgeTimestamp()), "+inf"),
                    new GetQueuesHandler(event, filterPatternResult.getOk(), countOnly));
        }
    }
}
