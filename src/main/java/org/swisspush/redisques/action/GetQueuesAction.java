package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetQueuesHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.*;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class GetQueuesAction extends AbstractQueueAction {

    public GetQueuesAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
            List<QueueConfiguration> queueConfigurations, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        getQueues(event, false, result);
    }

    protected void getQueues(Message<JsonObject> event, boolean countOnly, Result<Optional<Pattern>, String> filterPatternResult) {
        if (filterPatternResult.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, filterPatternResult.getErr()));
        } else {
            redisService.zrangebyscore(keyspaceHelper.getQueuesKey(), String.valueOf(getMaxAgeTimestamp()), "+inf")
                    .onComplete(response -> new GetQueuesHandler(event, filterPatternResult.getOk(), countOnly).handle(response));
        }
    }
}
