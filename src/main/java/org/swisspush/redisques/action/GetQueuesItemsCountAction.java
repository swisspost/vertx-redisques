package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetQueuesItemsCountHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Retrieve the size of the queues matching the given filter pattern
 */
public class GetQueuesItemsCountAction extends AbstractQueueAction {

    private final RedisQuesExceptionFactory exceptionFactory;
    private final Semaphore redisRequestQuota;

    public GetQueuesItemsCountAction(
            Vertx vertx,
            RedisService redisService,
            KeyspaceHelper keyspaceHelper,
            List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisRequestQuota,
            QueueStatisticsCollector queueStatisticsCollector,
            Logger log
    ) {
        super(vertx, redisService, keyspaceHelper, queueConfigurations,
                exceptionFactory, queueStatisticsCollector, log);
        this.exceptionFactory = exceptionFactory;
        this.redisRequestQuota = redisRequestQuota;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> filterPattern = MessageUtil.extractFilterPattern(event);
        if (filterPattern.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT)
                    .put(MESSAGE, filterPattern.getErr()));
        } else {
            redisService.zrangebyscore(keyspaceHelper.getQueuesKey(),
                    String.valueOf(getMaxAgeTimestamp()), "+inf").onComplete(response ->
                    new GetQueuesItemsCountHandler(vertx, event, filterPattern.getOk(),
                            keyspaceHelper.getQueuesPrefix(), redisService, exceptionFactory, redisRequestQuota).handle(response));
        }
    }

}
