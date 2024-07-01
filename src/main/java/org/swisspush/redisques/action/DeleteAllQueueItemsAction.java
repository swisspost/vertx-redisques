package org.swisspush.redisques.action;

import io.vertx.core.AsyncResult;
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
        var p = redisProvider.redis();
        p.onSuccess(redisAPI -> {
            redisAPI.del(Collections.singletonList(buildQueueKey(queue)), deleteReply -> {
                if (deleteReply.failed()) {
                    log.warn("Failed to deleteAllQueueItems. But we'll continue anyway",
                            new Exception(deleteReply.cause()));
                    // May we should 'fail()' here. But:
                    // 1st: We don't, to keep backward compatibility
                    // 2nd: We don't, to may unlock below.
                }
                queueStatisticsCollector.resetQueueFailureStatistics(queue, (Throwable ex, Void v) -> {
                    if (ex != null) log.warn("TODO_2958iouhj error handling", ex);
                });
                if (unlock) {
                    redisAPI.hdel(Arrays.asList(locksKey, queue), unlockReply -> {
                        if (unlockReply.failed()) {
                            log.warn("Failed to unlock queue '{}'. Will continue anyway",
                                    queue, unlockReply.cause());
                            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                        }
                        handleDeleteQueueReply(event, deleteReply);
                    });
                } else {
                    handleDeleteQueueReply(event, deleteReply);
                }
            });
        });
        p.onFailure(ex -> {
            log.error("Redis: Failed to delete all queue items", new Exception(ex));
            event.reply(createErrorReply());
        });
    }

    private void handleDeleteQueueReply(Message<JsonObject> event, AsyncResult<Response> reply) {
        if (reply.succeeded()) {
            event.reply(createOkReply().put(VALUE, reply.result().toLong()));
        } else {
            log.error("Failed to replyResultGreaterThanZero", new Exception(reply.cause()));
            event.reply(createErrorReply());
        }
    }
}
