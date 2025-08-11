package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.AddQueueItemHandler;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.Arrays;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class AddQueueItemAction extends AbstractQueueAction {

    public AddQueueItemAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
            String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        final String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        final String key1 = queuesPrefix + queueName;
        String valueAddItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        var p = redisProvider.redis();
        final QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);

        p.onSuccess(redisAPI -> redisAPI.rpush(Arrays.asList(key1, valueAddItem), asyncResult -> {
            final AddQueueItemHandler handler = new AddQueueItemHandler(event, exceptionFactory);
            if (asyncResult.succeeded()) {
                if (queueConfiguration != null && queueConfiguration.getMaxQueueEntries() > 0) {
                    final int maxQueueEntries = queueConfiguration.getMaxQueueEntries();
                    // we have limit set for this queue
                    log.debug("RedisQues Max queue entries {} found for queue {}", maxQueueEntries, queueName);
                    redisAPI.ltrim(key1, "-" + maxQueueEntries, "-1", handler);
                } else {
                    handler.handle(asyncResult);
                }
                return;
            }
            handler.handle(asyncResult);
        }));
        p.onFailure(ex -> handleFail(event, "Operation AddQueueItemAction failed", ex));
    }
}
