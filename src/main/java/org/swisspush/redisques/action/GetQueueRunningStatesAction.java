package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.util.RedisquesAPI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

public class GetQueueRunningStatesAction implements QueueAction {
    private final Logger log;
    // default time wait all nodes responses
    private static final long DEFAULT_WAIT_TIMEOUT = 2_000L;
    private final KeyspaceHelper keyspaceHelper;
    private final Vertx vertx;

    public GetQueueRunningStatesAction(Vertx vertx, KeyspaceHelper keyspaceHelper, Logger log) {
        this.vertx = vertx;
        this.keyspaceHelper =  keyspaceHelper;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject payload = event.body().getJsonObject(PAYLOAD);
        long lastUpdateWithInMs = payload.getLong(
                RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS,
                0L); // all

        int expectedReplies = payload.getInteger(
                RedisquesAPI.GET_QUEUE_RUNNING_STATES_EXPECTED_REPLIES,
                0); // unknown

        long maxWaitTimeout = payload.getLong(
                RedisquesAPI.GET_QUEUE_RUNNING_STATES_TIMEOUT,
                DEFAULT_WAIT_TIMEOUT);

        getAllRunningQueuesStatesFromAllVerticles(lastUpdateWithInMs, expectedReplies, maxWaitTimeout).onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                log.error("failed to get running queue states from all verticles.");
                event.reply(new JsonObject().put(STATUS, ERROR));
            } else {
                log.info("GetQueueRunningStatesAction received async result");
                event.reply(asyncResult.result());
            }
        });
    }

    /**
     * Gets the running queue states from all RedisQues verticle instances across the cluster.
     * <p>
     * This method creates a temporary event bus consumer with a unique reply address and
     * broadcasts a request to all queue state providers {@link org.swisspush.redisques.queue.QueueConsumerRunner}.
     * Each instance replies with its local running queue states, which are collected and returned as a single JSON result.
     * <p>
     * The collection completes when either:
     * <ul>
     *     <li>The expected number of replies has been received (if {@code expectedReplies > 0})</li>
     *     <li>The configured timeout is reached</li>
     * </ul>
     * The temporary consumer and timeout timer are automatically cleaned up after completion.
     *
     * @param lastUpdateWithInMs only include queue states updated within this time window
     *                           in milliseconds; {@code 0} means include all states
     * @param expectedReplies expected number of verticle replies before completing;
     *                        {@code 0} means the number of replies is unknown and the
     *                        method waits until timeout
     * @param timeoutMs maximum time in milliseconds to wait for replies;
     *                  values less than or equal to {@code 0} use the default timeout
     * @return a future containing a JSON object with {@link RedisquesAPI#PAYLOAD}
     *         containing the collected queue state responses
     */
    private Future<JsonObject> getAllRunningQueuesStatesFromAllVerticles(final long lastUpdateWithInMs, final int expectedReplies, final long timeoutMs) {
        final Promise<JsonObject> promise = Promise.promise();
        final List<JsonObject> results = Collections.synchronizedList(new ArrayList<>());
        final String replyAddress = keyspaceHelper.getQueueRunningStateReplyKey() + UUID.randomUUID();
        final AtomicLong timerId = new AtomicLong(-1L);
        final AtomicBoolean finished = new AtomicBoolean(false);
        MessageConsumer<JsonObject> consumer =
                vertx.eventBus().consumer(replyAddress);
        Runnable finish = () -> {
            if (!finished.compareAndSet(false, true)) {
                return;
            }
            long timerIdValue = timerId.get();
            if (timerIdValue >= 0) {
                vertx.cancelTimer(timerIdValue);
            }
            consumer.unregister();
            promise.tryComplete(new JsonObject()
                    .put(RedisquesAPI.PAYLOAD, new JsonArray(new ArrayList<>(results))));
        };
        consumer.handler(msg -> {
            results.add(msg.body());
            // received enough replies, finish immediately, no need to wait until timeout
            if (expectedReplies > 0 && (results.size() >= expectedReplies)) {
                finish.run();
            }
        });
        JsonObject requestJsonObject = new JsonObject()
                .put("reply", replyAddress)
                .put(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS, lastUpdateWithInMs);
        consumer.completionHandler(ar -> {
            if (ar.succeeded()) {
                long effectTimeout = timeoutMs;
                if (effectTimeout <= 0) {
                    effectTimeout = DEFAULT_WAIT_TIMEOUT;
                }
                // All callbacks here (completion handler, message handler, timer callback) are executed by Vert.x contexts.
                // The finished flag keeps finish() idempotent even if timeout and expected-replies completion race.
                timerId.set(vertx.setTimer(effectTimeout, id -> finish.run()));

                vertx.eventBus().publish(
                        keyspaceHelper.getQueueRunningStateKey(),
                        requestJsonObject
                );
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
}
