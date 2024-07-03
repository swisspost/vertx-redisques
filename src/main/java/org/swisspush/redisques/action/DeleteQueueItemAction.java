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
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.lset(keyLset, String.valueOf(indexLset), "TO_DELETE",
                        event1 -> {
                            if (event1.succeeded()) {
                                String keyLrem = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
                                redisAPI.lrem(keyLrem, "0", "TO_DELETE", replyLrem -> {
                                    if (replyLrem.failed()) {
                                        handleFail(event, "Failed to 'lrem' while deleteQueueItem", replyLrem.cause());
                                    } else {
                                        event.reply(createOkReply());
                                    }
                                });
                            } else {
                                if(checkRedisErrorCodes(event1.cause().getMessage())) {
                                    log.error("Failed to 'lset' while deleteQueueItem.", exceptionFactory.newException(event1.cause()));
                                    event.reply(createErrorReply());
                                } else{
                                    handleFail(event, "Failed to 'lset' while deleteQueueItem", event1.cause());
                                }
                            }
                        }))
                .onFailure(ex -> {
                    handleFail(event,"Operation DeleteQueueItemAction failed", ex);
                });
    }

    private boolean checkRedisErrorCodes(String message) {
        if(message == null) {
            return false;
        }
        return message.contains("no such key") || message.contains("index out of range");
    }
}
