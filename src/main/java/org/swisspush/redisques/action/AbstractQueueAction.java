package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
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

    protected final RedisService redisService;
    protected final Vertx vertx;
    protected final Logger log;
    protected final KeyspaceHelper keyspaceHelper;
    protected final List<QueueConfiguration> queueConfigurations;
    protected final RedisQuesExceptionFactory exceptionFactory;
    protected final QueueStatisticsCollector queueStatisticsCollector;

    public AbstractQueueAction(Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper, List<QueueConfiguration> queueConfigurations,
                               RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        this.vertx = vertx;
        this.redisService = redisService;
        this.keyspaceHelper = keyspaceHelper;
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

    protected void handleFail(Message<JsonObject> event, String msg, Throwable cause) {
        event.reply(exceptionFactory.newReplyException(msg, cause));
    }

    protected long getMaxAgeTimestamp() {
        return System.currentTimeMillis() - MAX_AGE_MILLISECONDS;
    }

    protected String buildQueueKey(String queue) {
        return keyspaceHelper.getQueuesPrefix() + queue;
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
        args.add(keyspaceHelper.getLocksKey());
        for (Response response : locks) {
            args.add(response.toString());
        }

        redisService.hdels(args).onComplete(delManyResult -> {
            if (delManyResult.succeeded()) {
                log.info("Successfully deleted {} locks", delManyResult.result());
                event.reply(createOkReply().put(VALUE, delManyResult.result().toLong()));
            } else {
                log.warn("failed to delete locks. Message: {}", delManyResult.cause().getMessage());
                event.reply(createErrorReply().put(MESSAGE, delManyResult.cause().getMessage()));
            }
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
        return redisService.zadd(keyspaceHelper.getQueuesKey(), queueName, String.valueOf(ts));
    }

    protected void notifyConsumer(final String queueName) {
        log.debug("RedisQues Notifying consumer of queue {}", queueName);
        final EventBus eb = vertx.eventBus();

        // Find the consumer to notify
        String key = keyspaceHelper.getConsumersPrefix() + queueName;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues notify consumer get: {}", key);
        }
        redisService.get(key).onComplete(event -> {
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
                eb.send(keyspaceHelper.getAddress() + "-consumers", queueName);
            } else {
                // Notify the registered consumer
                log.debug("RedisQues Notifying consumer {} to consume queue {}", consumer, queueName);
                eb.send(consumer, queueName);
            }
        });
    }

    /**
     * Try to trim a Queue by it states:
     * 1. Have a Consumer registered, will ask the consumer to process the trim request
     * 2. No Consumer registered, will trim it now.
     * 3. Failed to get Consumer, will do nothing, let the queue consumer itself to trim while process the queue
     * @param queueName
     * @return always succeeded future.
     */
    protected Future<Void> processTrimRequestByState(String queueName) {
        final Promise<Void> promise = Promise.promise();
        final QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
        if (queueConfiguration != null && queueConfiguration.getMaxQueueEntries() > 0) {
            final int maxQueueEntries = queueConfiguration.getMaxQueueEntries();
            final EventBus eb = vertx.eventBus();
            // we have limit set for this queue
            log.debug("RedisQues Max queue entries {} found for queue {}", maxQueueEntries, queueName);

            String consumerKey = keyspaceHelper.getConsumersPrefix() + queueName;
            if (log.isTraceEnabled()) {
                log.trace("RedisQues notify consumer get: {}", consumerKey);
            }

            redisService.get(consumerKey).onComplete(event -> {
                if (event.failed()) {
                    log.warn("Failed to get consumer for queue '{}'", queueName, event.cause());
                    //Skip Trim
                    promise.complete();
                    return;
                }
                String consumer = Objects.toString(event.result(), null);
                if (consumer == null) {
                    // No consumer for this queue, trim now
                    final String key = keyspaceHelper.getQueuesPrefix() + queueName;
                    redisService.ltrim(key,"-" + maxQueueEntries, "-1").onComplete(event1 -> {
                        if (event1.failed()) {
                            log.warn("Failed to trim queue '{}'", queueName, event1.cause());
                        }
                        promise.complete();
                    });
                } else {
                    // Notify the registered consumer to do the Trim
                    log.debug("RedisQues Notifying consumer {} to trim queue {}", consumer, queueName);
                    eb.send(keyspaceHelper.getTrimRequestKey(), queueName); // just send, no need to wait
                    promise.complete();
                }
            });
        } else {
            promise.complete();
        }
        return promise.future();
    }
}
