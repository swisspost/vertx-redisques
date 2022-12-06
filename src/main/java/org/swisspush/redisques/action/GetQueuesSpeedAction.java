package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.GetQueuesSpeedHandler;
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

/**
 * Retrieve the summarized queue speed of the requested queues
 */
public class GetQueuesSpeedAction extends AbstractQueueAction {

    public GetQueuesSpeedAction(Vertx vertx, LuaScriptManager luaScriptManager, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                                String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                                QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, luaScriptManager, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> filterPattern = MessageUtil.extractFilterPattern(event);
        getQueuesSpeed(event, filterPattern);
    }

    /**
     * Retrieve the summarized queue speed of the requested queues filtered by the
     * given filter pattern.
     */
    private void getQueuesSpeed(Message<JsonObject> event,
                                Result<Optional<Pattern>, String> filterPattern) {
        if (filterPattern.isErr()) {
            event.reply(QueueAction.createErrorReply().put(ERROR_TYPE, BAD_INPUT)
                    .put(MESSAGE, filterPattern.getErr()));
        } else {
            // retrieve all currently known queues from storage and pass this to the handler
            redisAPI.zrangebyscore(List.of(queuesKey, String.valueOf(getMaxAgeTimestamp()), "+inf"),
                    new GetQueuesSpeedHandler(event, filterPattern.getOk(),
                            queueStatisticsCollector));
        }
    }

}
