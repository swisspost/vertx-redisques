package org.swisspush.redisques.action;


import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.GetLockHandler;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

public class GetLockAction extends AbstractQueueAction {

    public GetLockAction(Vertx vertx, RedisProvider redisProvider, String address, String queuesKey, String queuesPrefix,
                         String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                         QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix, locksKey, queueConfigurations,
                queueStatisticsCollector, log);
    }


    @Override
    public void execute(Message<JsonObject> event) {
        final JsonObject body = event.body();
        if (null == body) {
            log.warn("Got msg with empty body from event bus. We'll run directly in " +
                    "a NullPointerException now. address={}  replyAddress={} ", event.address(), event.replyAddress());
            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
        }
        redisProvider.redis().onSuccess(redisAPI ->
                        redisAPI.hget(locksKey, body.getJsonObject(PAYLOAD).getString(QUEUENAME), new GetLockHandler(event)))
                .onFailure(replyErrorMessageHandler(event));
    }

}
