package org.swisspush.redisques.action;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public abstract class AbstractQueueAction implements QueueAction {

    protected RedisAPI redisAPI;

    protected Vertx vertx;

    protected Logger log;

    protected String address;
    protected String queuesKey;
    protected String queuesPrefix;
    protected String consumersPrefix;

    protected List<QueueConfiguration> queueConfigurations;

    protected QueueStatisticsCollector queueStatisticsCollector;

    public AbstractQueueAction(Vertx vertx, RedisAPI redisAPI, String address, String queuesKey,
                               String queuesPrefix, String consumersPrefix, List<QueueConfiguration> queueConfigurations,
                               QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        this.vertx = vertx;
        this.redisAPI = redisAPI;
        this.address = address;
        this.queuesKey = queuesKey;
        this.queuesPrefix = queuesPrefix;
        this.consumersPrefix = consumersPrefix;
        this.queueConfigurations = queueConfigurations;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.log = log;
    }

//    protected static JsonObject createOkReply() {
//        return new JsonObject().put(STATUS, OK);
//    }

//    protected static JsonObject createErrorReply() {
//        return new JsonObject().put(STATUS, ERROR);
//    }

    /**
     * find first matching Queue-Configuration
     *
     * @param queueName search first configuration for that queue-name
     * @return null when no queueConfiguration's RegEx matches given queueName - else the QueueConfiguration
     */
    protected QueueConfiguration findQueueConfiguration(String queueName) {
        for (QueueConfiguration queueConfiguration : queueConfigurations) {
            if (queueConfiguration.compiledPattern().matcher(queueName).matches()) {
                return queueConfiguration;
            }
        }
        return null;
    }

    protected void updateTimestamp(final String queueName, Handler<AsyncResult<Response>> handler) {
        long ts = System.currentTimeMillis();
        if (log.isTraceEnabled()) {
            log.trace("RedisQues update timestamp for queue: {} to: {}", queueName, ts);
        }
        if (handler == null) {
            redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName));
        } else {
            redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName), handler);
        }
    }

    protected void notifyConsumer(final String queueName) {
        log.debug("RedisQues Notifying consumer of queue {}", queueName);
        final EventBus eb = vertx.eventBus();

        // Find the consumer to notify
        String key = consumersPrefix + queueName;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues notify consumer get: {}", key);
        }
        redisAPI.get(key, event -> {
            if (event.failed()) {
                log.warn("Failed to get consumer for queue '{}'", queueName, event.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            String consumer = Objects.toString(event.result(), null);
            if (log.isTraceEnabled()) {
                log.trace("RedisQues got consumer: {}", consumer);
            }
            if (consumer == null) {
                // No consumer for this queue, let's make a peer become consumer
                if (log.isDebugEnabled()) {
                    log.debug("RedisQues Sending registration request for queue {}", queueName);
                }
                eb.send(address + "-consumers", queueName);
            } else {
                // Notify the registered consumer
                log.debug("RedisQues Notifying consumer {} to consume queue {}", consumer, queueName);
                eb.send(consumer, queueName);
            }
        });
    }
}
