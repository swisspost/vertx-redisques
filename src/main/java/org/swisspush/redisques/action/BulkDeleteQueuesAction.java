package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class BulkDeleteQueuesAction extends AbstractQueueAction {

    public BulkDeleteQueuesAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonArray queues = event.body().getJsonObject(PAYLOAD).getJsonArray(QUEUES);
        if (queues == null) {
            event.reply(createErrorReply().put(MESSAGE, "No queues to delete provided"));
            return;
        }

        if (queues.isEmpty()) {
            event.reply(createOkReply().put(VALUE, 0));
            return;
        }

        if (!jsonArrayContainsStringsOnly(queues)) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Queues must be string values"));
            return;
        }
        var p = redisProvider.redis();
        p.onSuccess(redisAPI -> redisAPI.del(buildQueueKeys(queues), delManyReply -> {
            queueStatisticsCollector.resetQueueStatistics(queues, (Throwable ex, Void v) -> {
                if (ex != null) log.warn("TODO_q93258hu38 error handling", ex);
            });
            if (delManyReply.succeeded()) {
                event.reply(createOkReply().put(VALUE, delManyReply.result().toLong()));
            } else {
                handleFail(event, "Failed to bulkDeleteQueues", delManyReply.cause());
            }
        }));
        p.onFailure(ex -> handleFail(event, "Operation BulkDeleteQueuesAction failed", ex));
    }

}
