package org.swisspush.redisques;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisUtils;
import org.swisspush.redisques.util.RedisquesConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class QueueRegistrationService {
    private static final Logger log = LoggerFactory.getLogger(QueueRegistrationService.class);
    private final RedisQuesExceptionFactory exceptionFactory;
    private final RedisProvider redisProvider;
    private final String consumersPrefix;
    private final int consumerLockTime;
    private final String consumerUid;

    QueueRegistrationService(RedisProvider redisProvider, RedisquesConfiguration modConfig, String consumerUid, RedisQuesExceptionFactory exceptionFactory) {
        this.redisProvider = redisProvider;
        this.consumersPrefix = modConfig.getRedisPrefix() + "consumers:";
        this.consumerLockTime = modConfig.getConsumerLockMultiplier() * modConfig.getRefreshPeriod(); // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        this.exceptionFactory = exceptionFactory;
        this.consumerUid = consumerUid;
    }

    void RegisterWithConsumer(String queueName, String value, boolean nx,
                              Handler<AsyncResult<Response>> handler) {
        final String consumersKey = consumersPrefix + queueName;
        JsonArray options = new JsonArray();
        options.add("EX").add(consumerLockTime);
        if (nx) {
            options.add("NX");
        }
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.send(
                        Command.SET, RedisUtils.toPayload(consumersKey, value, options).toArray(new String[0]))
                .onComplete(handler)).onFailure(throwable -> handler.handle(new FailedAsyncResult<>(throwable)));
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queuesKey key of the queue names
     * @param queueName name of the queue
     * @param handler   (optional) To get informed when done.
     */
    void updateTimestamp(final String queuesKey, final String queueName, Handler<AsyncResult<Response>> handler) {
        long ts = System.currentTimeMillis();
        log.trace("RedisQues update timestamp for queue: {} to: {}", queueName, ts);
        redisProvider.redis().onSuccess(redisAPI -> {
            if (handler == null) {
                redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName));
            } else {
                redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName), handler);
            }
        }).onFailure(throwable -> {
            log.warn("Redis: Error in updateTimestamp", throwable);
            if (handler != null) {
                handler.handle(new FailedAsyncResult<>(throwable));
            }
        });
    }

    /**
     * Remove queues from the sorted set that are timestamped before a limit time.
     *
     * @param limit limit timestamp
     */
    Future<Void> removeOldQueues(String queuesKey, long limit) {
        final Promise<Void> promise = Promise.promise();
        log.debug("Cleaning old queues");
        redisProvider.redis()
                .onSuccess(redisAPI -> {
                    redisAPI.zremrangebyscore(queuesKey, "-inf", String.valueOf(limit), event -> {
                        if (event.failed() && log.isWarnEnabled()) log.warn("TODO error handling",
                                exceptionFactory.newException("redisAPI.zremrangebyscore('" + queuesKey + "', '-inf', " + limit + ") failed",
                                        event.cause()));
                        promise.complete();
                    });
                })
                .onFailure(throwable -> {
                    log.warn("Redis: Failed to removeOldQueues", throwable);
                    promise.complete();
                });
        return promise.future();
    }

    /**
     * Caution: this may in some corner case violate the ordering for one
     * message.
     */
    void resetConsumers() {
        log.debug("RedisQues Resetting consumers");
        String keysPattern = consumersPrefix + "*";
        log.trace("RedisQues reset consumers keys: {}", keysPattern);
        redisProvider.redis().onSuccess(redisAPI -> redisAPI.keys(keysPattern, keysResult -> {
                    if (keysResult.failed() || keysResult.result() == null) {
                        log.error("Unable to get redis keys of consumers", keysResult.cause());
                        return;
                    }
                    Response keys = keysResult.result();
                    if (keys == null || keys.size() == 0) {
                        log.debug("No consumers found to reset");
                        return;
                    }
                    List<String> args = new ArrayList<>(keys.size());
                    for (Response response : keys) {
                        args.add(response.toString());
                    }
                    redisAPI.del(args, delManyResult -> {
                        if (delManyResult.succeeded()) {
                            if (log.isDebugEnabled())
                                log.debug("Successfully reset {} consumers", delManyResult.result().toLong());
                        } else {
                            log.error("Unable to delete redis keys of consumers");
                        }
                    });
                }))
                .onFailure(throwable -> log.error("Redis: Unable to get redis keys of consumers", throwable));
    }

    void refreshConsumerRegistration(String queueName, Handler<AsyncResult<Response>> handler) {
        log.debug("RedisQues Refreshing registration of queue {}, expire in {} s", queueName, consumerLockTime);
        String consumerKey = consumersPrefix + queueName;
        if (handler == null) {
            throw new RuntimeException("Handler must be set");
        } else {
            redisProvider.redis().onSuccess(redisAPI ->
                            redisAPI.expire(List.of(consumerKey, String.valueOf(consumerLockTime)), handler))
                    .onFailure(throwable -> {
                        log.warn("Redis: Failed to refresh registration of queue {}", queueName, throwable);
                        handler.handle(new FailedAsyncResult<>(throwable));
                    });
        }
    }

    Future<Void> unregisterConsumers(final Map<String, RedisQues.QueueState> queueList, boolean force) {
        final Promise<Void> result = Promise.promise();
        log.debug("RedisQues unregister consumers. force={}", force);
        final List<Future> futureList = new ArrayList<>(queueList.size());
        for (final Map.Entry<String, RedisQues.QueueState> entry : queueList.entrySet()) {
            final Promise<Void> promise = Promise.promise();
            futureList.add(promise.future());
            final String queue = entry.getKey();
            if (force || entry.getValue() == RedisQues.QueueState.READY) {
                log.trace("RedisQues unregister consumers queue: {}", queue);
                refreshConsumerRegistration(queue, event -> {
                    if (event.failed()) {
                        log.warn("TODO error handling", exceptionFactory.newException(
                                "refreshRegistration(" + queue + ") failed", event.cause()));
                    }
                    // Make sure that I am still the registered consumer
                    String consumerKey = consumersPrefix + queue;
                    log.trace("RedisQues unregister consumers get: {}", consumerKey);
                    redisProvider.redis().onSuccess(redisAPI -> redisAPI.get(consumerKey, getEvent -> {
                        if (getEvent.failed()) {
                            log.warn("Failed to retrieve consumer '{}'.", consumerKey, getEvent.cause());
                            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                        }
                        String consumer = Objects.toString(getEvent.result(), "");
                        log.trace("RedisQues unregister consumers get result: {}", consumer);
                        if (consumerUid.equals(consumer)) {
                            log.debug("RedisQues remove consumer: {}", consumerUid);
                            queueList.remove(queue);
                        }
                        promise.complete();
                    })).onFailure(throwable -> {
                        log.warn("Failed to retrieve consumer '{}'.", consumerKey, throwable);
                        promise.complete();
                    });
                });
            } else {
                promise.complete();
            }
        }
        CompositeFuture.all(futureList).onComplete(ev -> {
            if (ev.failed()) log.warn("TODO error handling", exceptionFactory.newException(ev.cause()));
            result.complete();
        });
        return result.future();
    }

    public String getConsumerKey(String queueName) {
        return consumersPrefix + queueName;
    }

    public String getConsumersPrefix() {
        return consumersPrefix;
    }
}
