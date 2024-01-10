package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.GetQueueItemsHandler;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class GetQueueItemsAction extends AbstractQueueAction {

    private static final int DEFAULT_MAX_QUEUEITEM_COUNT = 49;

    public GetQueueItemsAction(Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
                               String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                               QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        String keyListRange = queuesPrefix + queueName;
        int maxQueueItemCountIndex = getMaxQueueItemCountIndex(event.body().getJsonObject(PAYLOAD).getString(LIMIT));
        var p = redisProvider.redis();
        p.onSuccess(redisAPI -> redisAPI.llen(keyListRange, countReply -> {
            Long queueItemCount = countReply.result().toLong();
            if (countReply.succeeded() && queueItemCount != null) {
                redisAPI.lrange(keyListRange, "0", String.valueOf(maxQueueItemCountIndex),
                        new GetQueueItemsHandler(event, queueItemCount));
            } else {
                if( countReply.failed() ) {
                    log.warn("Operation getQueueItems failed. But I'll not notify my caller :)",
                            countReply.cause());
                    // IMO we should 'event.fail(countReply.cause())' here. But we don't, to keep
                    // backward compatibility.
                }
            }
        }));
        p.onFailure(ex -> {
            log.warn("Operation getQueueItems failed. But I'll not notify my caller :)", ex);
            // IMO we should 'event.fail(countReply.cause())' here. But we don't, to keep backward compatibility.
        });
    }

    private int getMaxQueueItemCountIndex(String limit) {
        int defaultMaxIndex = DEFAULT_MAX_QUEUEITEM_COUNT;
        if (limit != null) {
            try {
                int maxIndex = Integer.parseInt(limit) - 1;
                if (maxIndex >= 0) {
                    defaultMaxIndex = maxIndex;
                }
                log.info("use limit parameter " + maxIndex);
            } catch (NumberFormatException ex) {
                log.warn("Invalid limit parameter '{}' configured for max queue item count. Using default {}",
                        limit, DEFAULT_MAX_QUEUEITEM_COUNT);
            }
        }
        return defaultMaxIndex;
    }

}
