package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;

public class DeleteAllLocksAction extends AbstractQueueAction {

    public DeleteAllLocksAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.hkeys(locksKey, locksResult -> {
            if (locksResult.succeeded()) {
                Response locks = locksResult.result();
                deleteLocks(event, locks);
            } else {
                replyError(event, locksResult.cause());
            }
        })).onFailure(ex -> replyError(event, ex));
    }

    private void replyError(Message<JsonObject> event, Throwable ex) {
        if( log.isWarnEnabled() ) log.warn("failed to delete all locks.", new Exception(ex));
        event.reply(createErrorReply().put(MESSAGE, ex.getMessage()));
    }

}
