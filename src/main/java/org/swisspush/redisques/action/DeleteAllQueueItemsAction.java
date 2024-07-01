package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class DeleteAllQueueItemsAction extends AbstractQueueAction {

    public DeleteAllQueueItemsAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject payload = event.body().getJsonObject(PAYLOAD);
        boolean unlock = payload.getBoolean(UNLOCK, false);
        String queue = payload.getString(QUEUENAME);

        redisProvider.redis().onSuccess(redisAPI ->
                        redisAPI.del(Collections.singletonList(buildQueueKey(queue)))
                                .onSuccess(deleteReply -> {
                                    queueStatisticsCollector.resetQueueFailureStatistics(queue, (Throwable ex, Void v) -> {
                                        if (ex != null) log.warn("TODO_2958iouhj error handling", ex);
                                    });
                                    if (unlock) {
                                        redisAPI.hdel(Arrays.asList(locksKey, queue)).onSuccess(
                                                        unlockReply -> handleDeleteQueueReply(event, deleteReply))
                                                .onFailure(throwable -> handleFail(event, "Failed to unlock queue " + queue, throwable));
                                    } else {
                                        handleDeleteQueueReply(event, deleteReply);
                                    }
                                })
                                .onFailure(throwable -> handleFail(event, "Operation DeleteAllQueueItems failed", throwable)))
                .onFailure(throwable -> handleFail(event, "Operation DeleteAllQueueItems failed", throwable));

    }

    private void handleDeleteQueueReply(Message<JsonObject> event, Response reply) {
        event.reply(createOkReply().put(VALUE, reply.toLong()));
    }
}
