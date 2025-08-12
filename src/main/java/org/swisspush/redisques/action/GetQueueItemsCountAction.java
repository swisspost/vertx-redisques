package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.GetQueueItemsCountHandler;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

/**
 * Retrieve the number of items of the requested queue
 */
public class GetQueueItemsCountAction extends AbstractQueueAction {

    public GetQueueItemsCountAction(
            Vertx vertx, RedisProvider redisProvider, String address, String queuesKey,
            String queuesPrefix, String consumersPrefix, String locksKey,
            List<QueueConfiguration> queueConfigurations, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        final String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        final String queueKey = queuesPrefix + queueName;
        final QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
        var p = redisProvider.redis();
        p.onSuccess(redisAPI -> redisAPI.llen(queueKey, new GetQueueItemsCountHandler(event, queueConfiguration)));
        p.onFailure(ex -> replyErrorMessageHandler(event).handle(ex));
    }

}
