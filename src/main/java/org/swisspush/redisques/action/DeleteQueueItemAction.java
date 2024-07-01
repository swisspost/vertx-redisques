package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class DeleteQueueItemAction extends AbstractQueueAction {

    public DeleteQueueItemAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String keyLset = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexLset = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);

        redisProvider.redis().onSuccess(redisAPI -> redisAPI.lset(keyLset, String.valueOf(indexLset), "TO_DELETE").onSuccess(lsetResponse -> {
            String keyLrem = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
            redisAPI.lrem(keyLrem, "0", "TO_DELETE").onSuccess(lremResponse -> event.reply(createOkReply())).onFailure(throwable -> {
                log.warn("Redis 'lrem' command failed", new Exception(throwable));
                event.reply(createErrorReply());
            });
        }).onFailure(throwable -> {
            log.error("Failed to 'lset' while deleteQueueItem.", new Exception(throwable));
            event.reply(createErrorReply());
        })).onFailure(throwable -> {
            log.error("Redis: Failed to deleteQueueItem.", new Exception(throwable));
            event.reply(createErrorReply());
        });
    }
}
