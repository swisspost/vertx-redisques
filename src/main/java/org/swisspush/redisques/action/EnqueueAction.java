package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.Arrays;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class EnqueueAction extends AbstractQueueAction {


    public EnqueueAction(Vertx vertx, RedisAPI redisAPI, String address, String queuesKey, String queuesPrefix,
                         String consumersPrefix, List<QueueConfiguration> queueConfigurations,
                         QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, redisAPI, address, queuesKey, queuesPrefix, consumersPrefix, queueConfigurations,
                queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        updateTimestamp(queueName, null);
        String keyEnqueue = queuesPrefix + queueName;
        String valueEnqueue = event.body().getString(MESSAGE);
        redisAPI.rpush(Arrays.asList(keyEnqueue, valueEnqueue), event2 -> {
            JsonObject reply = new JsonObject();
            if (event2.succeeded()) {
                if (log.isDebugEnabled()) {
                    log.debug("RedisQues Enqueued message into queue {}", queueName);
                }
                long queueLength = event2.result().toLong();
                notifyConsumer(queueName);
                reply.put(STATUS, OK);
                reply.put(MESSAGE, "enqueued");

                // feature EN-queue slow-down (the larger the queue the longer we delay "OK" response)
                long delayReplyMillis = 0;
                QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
                if (queueConfiguration != null) {
                    float enqueueDelayFactorMillis = queueConfiguration.getEnqueueDelayFactorMillis();
                    if (enqueueDelayFactorMillis > 0f) {
                        // minus one as we need the queueLength _before_ our en-queue here
                        delayReplyMillis = (long) ((queueLength - 1) * enqueueDelayFactorMillis);
                        int max = queueConfiguration.getEnqueueMaxDelayMillis();
                        if (max > 0 && delayReplyMillis > max) {
                            delayReplyMillis = max;
                        }
                    }
                }
                if (delayReplyMillis > 0) {
                    vertx.setTimer(delayReplyMillis, timeIsUp -> event.reply(reply));
                } else {
                    event.reply(reply);
                }
                queueStatisticsCollector.setQueueBackPressureTime(queueName, delayReplyMillis);
            } else {
                String message = "RedisQues QUEUE_ERROR: Error while enqueueing message into queue " + queueName;
                log.error(message, event2.cause());
                reply.put(STATUS, ERROR);
                reply.put(MESSAGE, message);
                event.reply(reply);
            }
        });
    }
}
