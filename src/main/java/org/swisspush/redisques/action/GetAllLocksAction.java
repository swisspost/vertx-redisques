package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetAllLocksHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.*;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class GetAllLocksAction extends AbstractQueueAction {

    public GetAllLocksAction(
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
        if (result.isOk()) {
            redisService.hkeys(keyspaceHelper.getLocksKey()).onComplete(response -> new GetAllLocksHandler(exceptionFactory, event, result.getOk()).handle(response))
                    .onFailure(throwable -> handleFail(event, "Operation GetAllLocks failed", throwable));
        } else {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, result.getErr()));
        }
    }

}
