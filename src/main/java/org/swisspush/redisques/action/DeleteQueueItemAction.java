package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class DeleteQueueItemAction extends AbstractQueueAction {

    public DeleteQueueItemAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, queueStatisticsCollector, log);
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
                                log.warn("Redis 'lrem' command failed. But will continue anyway.",
                                        new Exception(replyLrem.cause()));
                                // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                            }
                            event.reply(createOkReply());
                        });
                    } else {
                        log.error("Failed to 'lset' while deleteQueueItem.", new Exception(event1.cause()));
                        event.reply(createErrorReply());
                    }
                })).onFailure(ex -> {
                    log.error("Redis: Failed to deleteQueueItem.", new Exception(ex));
                    event.reply(createErrorReply());
                });
    }
}
