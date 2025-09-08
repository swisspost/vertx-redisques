package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;

public class DeleteAllLocksAction extends AbstractQueueAction {

    public DeleteAllLocksAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        redisService.hkeys(keyspaceHelper.getLocksKey()).onComplete(locksResult -> {
            if (locksResult.succeeded()) {
                Response locks = locksResult.result();
                deleteLocks(event, locks);
            } else {
                replyError(event, locksResult.cause());
            }
        });
    }

    private void replyError(Message<JsonObject> event, Throwable ex) {
        if (log.isWarnEnabled()) log.warn("failed to delete all locks.", new Exception(ex));
        event.reply(createErrorReply().put(MESSAGE, ex.getMessage()));
    }

}
