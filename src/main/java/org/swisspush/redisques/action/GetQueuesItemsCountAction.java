package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetQueuesItemsCountHandler;
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
            RedisProvider redisProvider,
            String address,
            String queuesKey,
            String queuesPrefix,
            String consumersPrefix,
            String locksKey,
            List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisRequestQuota,
            QueueStatisticsCollector queueStatisticsCollector,
            Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
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
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.zrangebyscore(List.of(queuesKey,
                                    String.valueOf(getMaxAgeTimestamp()), "+inf"),
                            new GetQueuesItemsCountHandler(vertx, event, filterPattern.getOk(),
                                    queuesPrefix, redisProvider, exceptionFactory, redisRequestQuota)))
                    .onFailure(ex -> replyErrorMessageHandler(event).handle(ex));
        }
    }

}
