package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public abstract class AbstractQueueAction implements QueueAction {

    private static final int MAX_AGE_MILLISECONDS = 120000; // 120 seconds

    protected final RedisProvider redisProvider;
    protected final Vertx vertx;
    protected final Logger log;
    protected final String address;
    protected final String queuesKey;
    protected final String queuesPrefix;
    protected final String consumersPrefix;
    protected final String locksKey;
    protected final List<QueueConfiguration> queueConfigurations;
    protected final RedisQuesExceptionFactory exceptionFactory;
    protected final QueueStatisticsCollector queueStatisticsCollector;

    public AbstractQueueAction(Vertx vertx, RedisProvider redisProvider, String address, String queuesKey,
                               String queuesPrefix, String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                               RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        this.vertx = vertx;
        this.redisProvider = redisProvider;
        this.address = address;
        this.queuesKey = queuesKey;
        this.queuesPrefix = queuesPrefix;
        this.consumersPrefix = consumersPrefix;
        this.locksKey = locksKey;
        this.queueConfigurations = queueConfigurations;
        this.exceptionFactory = exceptionFactory;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.log = log;
    }

    protected Handler<Throwable> replyErrorMessageHandler(Message<JsonObject> event) {
        return ex -> {
            log.warn("Concealed error", new Exception(ex));
            event.reply(new JsonObject().put(STATUS, ERROR));
        };
    }

    protected void handleFail(Message<JsonObject> event, String message, Throwable throwable) {
        log.warn(message, exceptionFactory.newException(throwable));
        event.fail(0, throwable.getMessage());
    }

    protected long getMaxAgeTimestamp() {
        return System.currentTimeMillis() - MAX_AGE_MILLISECONDS;
    }

    protected String buildQueueKey(String queue) {
        return queuesPrefix + queue;
    }

    protected List<String> buildQueueKeys(JsonArray queues) {
        if (queues == null) {
            return null;
        }
        final int size = queues.size();
        List<String> queueKeys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String queue = queues.getString(i);
            queueKeys.add(buildQueueKey(queue));
        }
        return queueKeys;
    }

    protected boolean jsonArrayContainsStringsOnly(JsonArray array) {
        for (Object obj : array) {
            if (!(obj instanceof String)) {
                return false;
            }
        }
        return true;
    }

    protected JsonObject extractLockInfo(String requestedBy) {
        if (requestedBy == null) {
            return null;
        }
        JsonObject lockInfo = new JsonObject();
        lockInfo.put(REQUESTED_BY, requestedBy);
        lockInfo.put(TIMESTAMP, System.currentTimeMillis());
        return lockInfo;
    }

    protected List<String> buildLocksItems(String locksKey, JsonArray lockNames, JsonObject lockInfo) {
        List<String> list = new ArrayList<>();
        list.add(locksKey);
        String lockInfoStr = lockInfo.encode();
        for (int i = 0; i < lockNames.size(); i++) {
            String lock = lockNames.getString(i);
            list.add(lock);
            list.add(lockInfoStr);
        }
        return list;
    }


    protected void deleteLocks(Message<JsonObject> event, Response locks) {
        if (locks == null || locks.size() == 0) {
            event.reply(createOkReply().put(VALUE, 0));
            return;
        }

        List<String> args = new ArrayList<>();
        args.add(locksKey);
        for (Response response : locks) {
            args.add(response.toString());
        }

        redisProvider.redis().onSuccess(redisAPI -> redisAPI.hdel(args, delManyResult -> {
            if (delManyResult.succeeded()) {
                log.info("Successfully deleted {} locks", delManyResult.result());
                event.reply(createOkReply().put(VALUE, delManyResult.result().toLong()));
            } else {
                log.warn("failed to delete locks. Message: {}", delManyResult.cause().getMessage());
                event.reply(createErrorReply().put(MESSAGE, delManyResult.cause().getMessage()));
            }
        })).onFailure(throwable -> {
            log.warn("Redis: failed to delete locks", throwable);
            event.reply(createErrorReply().put(MESSAGE, throwable.getMessage()));
        });
    }


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
    protected Future<Response> updateTimestamp(final String queueName) {
        long ts = System.currentTimeMillis();
        if (log.isTraceEnabled()) {
            log.trace("RedisQues update timestamp for queue: {} to: {}", queueName, ts);
        }
        return redisProvider.redis().compose(redisAPI -> redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName)));
    }

    protected void notifyConsumer(final String queueName) {
        log.debug("RedisQues Notifying consumer of queue {}", queueName);
        final EventBus eb = vertx.eventBus();

        // Find the consumer to notify
        String key = consumersPrefix + queueName;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues notify consumer get: {}", key);
        }
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(key, event -> {
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
        })).onFailure(throwable -> {
            log.warn("Redis: Failed to get consumer for queue '{}'", queueName, throwable);
            // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
        });
    }
}
