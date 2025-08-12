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
        String key1 = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        String valueAddItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        var p = redisProvider.redis();
        p.onSuccess(redisAPI -> redisAPI.rpush(Arrays.asList(key1, valueAddItem), new AddQueueItemHandler(event, exceptionFactory)));
        p.onFailure(ex -> handleFail(event, "Operation AddQueueItemAction failed", ex));
    }

}
