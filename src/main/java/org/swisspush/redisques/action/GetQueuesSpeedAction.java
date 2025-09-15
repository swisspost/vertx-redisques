package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetQueuesSpeedHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.*;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Retrieve the summarized queue speed of the requested queues
 */
public class GetQueuesSpeedAction extends AbstractQueueAction {

    public GetQueuesSpeedAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
            List<QueueConfiguration> queueConfigurations, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
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
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT)
                    .put(MESSAGE, filterPattern.getErr()));
        } else {
            redisService.zrangebyscore(keyspaceHelper.getQueuesKey(), String.valueOf(getMaxAgeTimestamp()), "+inf").onComplete(response ->
                    new GetQueuesSpeedHandler(event, filterPattern.getOk(),
                            queueStatisticsCollector).handle(response));
        }
    }
}
