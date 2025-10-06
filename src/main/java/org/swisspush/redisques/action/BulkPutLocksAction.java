package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.PutLockHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class BulkPutLocksAction extends AbstractQueueAction {

    public BulkPutLocksAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonArray locks = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (locks == null || locks.isEmpty()) {
            event.reply(createErrorReply().put(MESSAGE, "No locks to put provided"));
            return;
        }

        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo == null) {
            event.reply(createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
            return;
        }

        if (!jsonArrayContainsStringsOnly(locks)) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Locks must be string values"));
            return;
        }
        redisService.hmset(buildLocksItems(keyspaceHelper.getLocksKey(), locks, lockInfo)).onComplete(response ->
                new PutLockHandler(event, exceptionFactory).handle(response))
                .onFailure(ex -> replyErrorMessageHandler(event).handle(ex));
    }

}
