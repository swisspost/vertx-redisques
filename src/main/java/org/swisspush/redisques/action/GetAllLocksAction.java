package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetAllLocksHandler;
import org.swisspush.redisques.util.*;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class GetAllLocksAction extends AbstractQueueAction {

    public GetAllLocksAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey,
            String queuesPrefix, String consumersPrefix, String locksKey,
            List<QueueConfiguration> queueConfigurations, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        if (result.isOk()) {
            redisProvider.redis()
                    .onSuccess(redisAPI -> redisAPI.hkeys(locksKey, new GetAllLocksHandler(exceptionFactory, event, result.getOk())))
                    .onFailure(throwable -> handleFail(event, "Operation GetAllLocks failed", throwable));
        } else {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, result.getErr()));
        }
    }

}
