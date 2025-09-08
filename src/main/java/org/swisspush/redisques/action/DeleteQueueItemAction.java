package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class DeleteQueueItemAction extends AbstractQueueAction {

    public DeleteQueueItemAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String keyLset = keyspaceHelper.getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexLset = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        redisService.lset(keyLset, String.valueOf(indexLset), "TO_DELETE").onComplete(
                event1 -> {
                    if (event1.succeeded()) {
                        String keyLrem = keyspaceHelper.getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
                        redisService.lrem(keyLrem, "0", "TO_DELETE").onComplete(replyLrem -> {
                            if (replyLrem.failed()) {
                                handleFail(event, "Failed to 'lrem' while deleteQueueItem", replyLrem.cause());
                            } else {
                                event.reply(createOkReply());
                            }
                        });
                    } else {
                        if (checkRedisErrorCodes(event1.cause().getMessage())) {
                            log.error("Failed to 'lset' while deleteQueueItem.", exceptionFactory.newException(event1.cause()));
                            event.reply(createErrorReply());
                        } else {
                            handleFail(event, "Failed to 'lset' while deleteQueueItem", event1.cause());
                        }
                    }
                });
    }

    private boolean checkRedisErrorCodes(String message) {
        if (message == null) {
            return false;
        }
        return message.contains("no such key") || message.contains("index out of range");
    }
}
