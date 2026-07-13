package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.HandlerUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisClusterUtil;

import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_NAME;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_SIZE;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUES;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.buildGetQueueRunningStates;


public class GetQueuesItemsCountHandler implements Handler<AsyncResult<Response>> {
    private final Logger log = LoggerFactory.getLogger(GetQueuesItemsCountHandler.class);
    private final static long RUNNING_STATES_SIZE_UPDATE_WITHIN_MS = 30_000;
    private final Vertx vertx;
    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final String queuesPrefix;
    private final RedisService redisService;
    private final UpperBoundParallel upperBoundParallel;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final Semaphore redisRequestQuota;
    private final int redisRequestQuotaAcquireRetryTime;
    private final QueueStatisticsCollector queueStatisticsCollector;
    private final KeyspaceHelper keyspaceHelper;
    private final boolean reloadSize;

    public GetQueuesItemsCountHandler(
            Vertx vertx,
            Message<JsonObject> event,
            Optional<Pattern> filterPattern,
            String queuesPrefix,
            RedisService redisService,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisRequestQuota,
            int redisRequestQuotaAcquireRetryTime,
            QueueStatisticsCollector queueStatisticsCollector,
            KeyspaceHelper keyspaceHelper,
            boolean reloadSize

    ) {
        this.vertx = vertx;
        this.event = event;
        this.filterPattern = filterPattern;
        this.queuesPrefix = queuesPrefix;
        this.redisService = redisService;
        this.upperBoundParallel = new UpperBoundParallel(vertx, exceptionFactory);
        this.exceptionFactory = exceptionFactory;
        this.redisRequestQuota = redisRequestQuota;
        this.redisRequestQuotaAcquireRetryTime = redisRequestQuotaAcquireRetryTime;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.keyspaceHelper = keyspaceHelper;
        this.reloadSize = reloadSize;
    }

    @Override
    public void handle(AsyncResult<Response> handleQueues) {
        if (!handleQueues.succeeded()) {
            log.warn("Concealed error", exceptionFactory.newException(handleQueues.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
            return;
        }
        vertx.eventBus().<JsonObject>request(keyspaceHelper.getAddress(),
                buildGetQueueRunningStates(RUNNING_STATES_SIZE_UPDATE_WITHIN_MS, 0, 0)).onComplete(asyncRunningStatesResult -> {
            final Map<String, Long> queueSizeFromStatistics = new HashMap<>();

            if (!reloadSize) {
                if (asyncRunningStatesResult.failed()) {
                    log.warn("Can't get queues states, so all queue size will fetch from Redis", exceptionFactory.newException(asyncRunningStatesResult.cause()));
                } else {
                    queueSizeFromStatistics.putAll(QueueStatisticsCollector.mergeQueueSizeFromAllQueueRunningStates(asyncRunningStatesResult.result().body().getJsonArray(PAYLOAD)));
                }
            } else {
                log.debug("Force reload queue size from Redis, so ignore queue size from statistics");
            }

            List<String> queueKeys = HandlerUtil.filterByPattern(handleQueues.result(), filterPattern);
            Map<String, String> keyQueuePairs = new LinkedHashMap<>();

            // Remove queues from statistics that don't exist in filtered queueKeys
            queueSizeFromStatistics.keySet().removeIf(queue -> !queueKeys.contains(queue));

            // Remove queues from queueKeys if statistics already contain them
            queueKeys.removeIf(queueSizeFromStatistics::containsKey);

            // so we only look for size of missing queues
            for (String key : queueKeys) {
                keyQueuePairs.put(queuesPrefix + key, key);
            }
            keyQueuePairs = RedisClusterUtil.groupMapBySlot(keyQueuePairs);

            List<Map.Entry<String, String>> finalKeyQueuePairs = new ArrayList<>(keyQueuePairs.entrySet());
            var ctx = new Object() {
                Iterator<Map.Entry<String, String>> iter;
                // order all keys by slot so can process almost key in the same slot at once
                List<Map.Entry<String, String>> keyQueuePair = finalKeyQueuePairs;
                int iNumberResult;
                int[] queueLengths;
            };
            if (ctx.keyQueuePair.isEmpty() && queueSizeFromStatistics.isEmpty()) {
                log.debug("Queue count evaluation with empty queues");
                event.reply(new JsonObject().put(STATUS, OK).put(QUEUES, new JsonArray()));
                return;
            }

            if (!queueSizeFromStatistics.isEmpty()) {
                // all from statistics
                JsonArray result = new JsonArray();
                queueSizeFromStatistics.forEach((queueName, queueSize) -> result.add(new JsonObject()
                        .put(MONITOR_QUEUE_NAME, queueName)
                        .put(MONITOR_QUEUE_SIZE, queueSize)));
                JsonObject json = new JsonObject().put(STATUS, OK).put(QUEUES, result);
                event.reply(json);
                return;
            }

            if (redisRequestQuota.availablePermits() <= 0) {
                event.reply(exceptionFactory.newReplyException(429,
                        "Too many simultaneous '" + GetQueuesItemsCountHandler.class.getSimpleName() + "' requests in progress", null));
                return;
            }

            Future.succeededFuture().compose(o -> {
                ctx.queueLengths = new int[ctx.keyQueuePair.size()];
                ctx.iter = ctx.keyQueuePair.iterator();
                var p = Promise.<Void>promise();
                upperBoundParallel.request(redisRequestQuota, redisRequestQuotaAcquireRetryTime, null, new UpperBoundParallel.Mentor<Void>() {
                    @Override
                    public boolean runOneMore(BiConsumer<Throwable, Void> onLLenDone, Void unused) {
                        if (!ctx.iter.hasNext()) {
                            return false;
                        }
                        List<String> keyBatch = new ArrayList<>(RedisService.MAX_COMMANDS_IN_BATCH);
                        List<Integer> resultIndexes = new ArrayList<>(RedisService.MAX_COMMANDS_IN_BATCH);
                        int count = 0;
                        while (ctx.iter.hasNext() && count < RedisService.MAX_COMMANDS_IN_BATCH) {
                            Map.Entry<String, String> queue = ctx.iter.next();
                            int index = ctx.iNumberResult++;
                            keyBatch.add(queue.getKey());
                            resultIndexes.add(index);
                            count++;
                        }
                        redisService.clusterSafeBatch(Command.LLEN, keyBatch, List.of())
                                .onSuccess(responses -> {
                                    for (int i = 0; i < responses.size(); i++) {
                                        ctx.queueLengths[resultIndexes.get(i)] =
                                                responses.get(i).toInteger();
                                    }
                                    onLLenDone.accept(null, null);

                                })
                                .onFailure(ex -> onLLenDone.accept(ex, null));
                        return ctx.iter.hasNext();
                    }

                    @Override
                    public boolean onError(Throwable ex, Void ctx_) {
                        log.error("Unexpected queue length result", exceptionFactory.newException(ex));
                        event.reply(new JsonObject().put(STATUS, ERROR));
                        return false;
                    }

                    @Override
                    public void onDone(Void ctx_) {
                        p.complete();
                    }
                });
                return p.future();
            }).compose((Void v) -> {
                /*going to waste another threads time to produce those garbage objects*/
                return vertx.executeBlocking((Promise<JsonObject> workerPromise) -> {
                    assert !Thread.currentThread().getName().toUpperCase().contains("EVENTLOOP");
                    long beginEpchMs = currentTimeMillis();

                    JsonArray result = new JsonArray();
                    // add queue with size from LLEN
                    for (int i = 0; i < ctx.queueLengths.length; ++i) {
                        String queueName = ctx.keyQueuePair.get(i).getValue();
                        result.add(new JsonObject()
                                .put(MONITOR_QUEUE_NAME, queueName)
                                .put(MONITOR_QUEUE_SIZE, ctx.queueLengths[i]));
                    }
                    // add queue with size from Statistics
                    queueSizeFromStatistics.forEach((queueName, queueSize) -> result.add(new JsonObject()
                            .put(MONITOR_QUEUE_NAME, queueName)
                            .put(MONITOR_QUEUE_SIZE, queueSize)));

                    var obj = new JsonObject().put(STATUS, OK).put(QUEUES, result);
                    long jsonCreateDurationMs = currentTimeMillis() - beginEpchMs;
                    if (jsonCreateDurationMs > 10) {
                        log.info("Creating JSON with {} entries did block this thread for {}ms",
                                ctx.queueLengths.length, jsonCreateDurationMs);
                    } else {
                        log.debug("Creating JSON with {} entries did block this thread for {}ms",
                                ctx.queueLengths.length, jsonCreateDurationMs);
                    }
                    workerPromise.complete(obj);
                }, false);
            }).onSuccess((JsonObject json) -> {
                log.trace("call event.reply(json)");
                event.reply(json);
            }).onFailure((Throwable ex) -> {
                log.warn("Redis: Failed to get queue length.", exceptionFactory.newException(ex));
                event.reply(new JsonObject().put(STATUS, ERROR));
            });
        });
    }
}
