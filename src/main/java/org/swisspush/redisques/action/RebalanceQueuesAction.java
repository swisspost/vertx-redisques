package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.QueueRebalancePlanner;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.MessageUtil;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.Result;
import org.swisspush.redisques.util.RedisquesAPI;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.BAD_INPUT;
import static org.swisspush.redisques.util.RedisquesAPI.DRY_RUN;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR_TYPE;
import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;
import static org.swisspush.redisques.util.RedisquesAPI.MAX_MOVES_PER_RUN;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

public class RebalanceQueuesAction extends AbstractQueueAction {
    private static final int DEFAULT_MAX_MOVES_PER_RUN = 100;
    private static final int HARD_MAX_MOVES_PER_RUN = 100;
    private static final String SKIP_OWNER_CHANGED = "owner-changed";
    private static final String SKIP_OWNER_LOOKUP_FAILED = "owner-lookup-failed";
    private static final String SKIP_NOT_READY = "not-ready";
    private static final String SKIP_CLAIM_FAILED = "claim-failed";
    private static final String SKIP_RELEASE_FAILED = "release-failed";
    private static final long DEFAULT_RUNNING_STATE_TIMEOUT_MS = 2_000L;
    private static final String REBALANCE_ACTION = "action";
    private static final String REBALANCE_ACTION_RELEASE = "release";
    private static final String REBALANCE_ACTION_CLAIM = "claim";

    public RebalanceQueuesAction(Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
                                 QueueConfigurationProvider queueConfigurationProvider,
                                 RedisquesConfigurationProvider redisquesConfigurationProvider,
                                 RedisQuesExceptionFactory exceptionFactory,
                                 QueueStatisticsCollector queueStatisticsCollector,
                                 Logger log) {
        super(vertx, redisService, keyspaceHelper, queueConfigurationProvider, redisquesConfigurationProvider, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> filterPatternResult = MessageUtil.extractFilterPattern(event);
        if (filterPatternResult.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, filterPatternResult.getErr()));
            return;
        }

        JsonObject payload = event.body().getJsonObject(PAYLOAD, new JsonObject());
        Integer requestedMaxMoves = payload.getInteger(MAX_MOVES_PER_RUN);
        int maxMovesPerRun = requestedMaxMoves == null ? DEFAULT_MAX_MOVES_PER_RUN : requestedMaxMoves;
        if (maxMovesPerRun <= 0) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "maxMovesPerRun must be between 1 and 100"));
            return;
        }
        if (maxMovesPerRun > HARD_MAX_MOVES_PER_RUN) {
            maxMovesPerRun = HARD_MAX_MOVES_PER_RUN;
        }
        final int effectiveMaxMovesPerRun = maxMovesPerRun;

        boolean dryRun = payload.getBoolean(DRY_RUN, false);
        fetchQueueScope(filterPatternResult.getOk())
                .compose(queueScope -> fetchExpectedAliveConsumerCount()
                        .compose(expectedAliveConsumers -> fetchRunningStates(expectedAliveConsumers)
                                .compose(runningStates -> {
                                    if (runningStates.size() < expectedAliveConsumers) {
                                        return Future.failedFuture("Incomplete running state replies. Expected "
                                                + expectedAliveConsumers + " but received " + runningStates.size());
                                    }
                                    return Future.succeededFuture(new PlanningContext(queueScope, runningStates));
                                })))
                .compose(context -> {
                    PlannerInput plannerInput = buildPlannerInput(context.runningStates, context.queueScope);
                    QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner()
                            .computePlan(plannerInput.loadByConsumer, plannerInput.readyQueuesByConsumer, effectiveMaxMovesPerRun);
                    if (dryRun) {
                        return Future.succeededFuture(createSuccessReply(plan.getMoves().size(), 0, Collections.emptyMap()));
                    }
                    return executePlan(plan, context.runningStates).map(result -> createSuccessReply(plan.getMoves().size(), result.executedMoves, result.reasonsByQueue));
                })
                .onSuccess(event::reply)
                .onFailure(throwable -> handleFail(event, "Failed to rebalance queues", throwable));
    }

    protected Future<Set<String>> fetchQueueScope(Optional<Pattern> filterPattern) {
        return redisService.zrangebyscore(keyspaceHelper.getQueuesKey(), String.valueOf(getMaxAgeTimestamp()), "+inf")
                .map(response -> toQueueScope(response, filterPattern));
    }

    protected Future<Integer> fetchExpectedAliveConsumerCount() {
        long aliveConsumerTtlMs = redisquesConfigurationProvider.configuration().getRefreshPeriod() * 1000L * 2;
        long expireScore = System.currentTimeMillis() - aliveConsumerTtlMs;
        List<Request> requests = new ArrayList<>(2);
        requests.add(Request.cmd(Command.ZREMRANGEBYSCORE)
                .arg(keyspaceHelper.getAliveConsumersKey())
                .arg("0")
                .arg(String.valueOf(expireScore)));
        requests.add(Request.cmd(Command.ZRANGE)
                .arg(keyspaceHelper.getAliveConsumersKey())
                .arg("0")
                .arg("-1"));
        return redisService.batch(requests).map(responses -> {
            if (responses == null || responses.size() < 2 || responses.get(1) == null) {
                return 0;
            }
            return responses.get(1).size();
        });
    }

    protected Future<JsonArray> fetchRunningStates(int expectedReplies) {
        Promise<JsonArray> promise = Promise.promise();
        List<JsonObject> results = Collections.synchronizedList(new ArrayList<>());
        String replyAddress = keyspaceHelper.getQueueRunningStateReplyKey() + UUID.randomUUID();
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(replyAddress);
        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicLong timerId = new AtomicLong(-1L);

        Runnable finish = () -> {
            if (!finished.compareAndSet(false, true)) {
                return;
            }
            long currentTimerId = timerId.get();
            if (currentTimerId >= 0L) {
                vertx.cancelTimer(currentTimerId);
            }
            consumer.unregister();
            promise.tryComplete(new JsonArray(new ArrayList<>(results)));
        };

        consumer.handler(message -> {
            results.add(message.body());
            if (expectedReplies > 0 && results.size() >= expectedReplies) {
                finish.run();
            }
        });
        consumer.completionHandler(ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                return;
            }
            timerId.set(vertx.setTimer(DEFAULT_RUNNING_STATE_TIMEOUT_MS, id -> finish.run()));
            vertx.eventBus().publish(keyspaceHelper.getQueueRunningStateKey(), new JsonObject()
                    .put("reply", replyAddress)
                    .put(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS, 0L)
                    .put(RedisquesAPI.GET_QUEUE_RUNNING_STATES_EXPECTED_REPLIES, expectedReplies));
        });

        return promise.future();
    }

    protected Future<Boolean> requestSourceRelease(String sourceConsumerId, String queueName, String expectedOwner) {
        String address = keyspaceHelper.getAddress() + "-rebalance-control:" + sourceConsumerId;
        JsonObject request = new JsonObject()
                .put(REBALANCE_ACTION, REBALANCE_ACTION_RELEASE)
                .put("queueName", queueName)
                .put("expectedOwner", expectedOwner);
        return vertx.eventBus().<Boolean>request(address, request)
                .map(message -> message.body() != null && message.body());
    }

    protected Future<Boolean> requestTargetClaim(String targetConsumerId, String queueName, String expectedOwner) {
        String address = keyspaceHelper.getAddress() + "-rebalance-control:" + targetConsumerId;
        JsonObject request = new JsonObject()
                .put(REBALANCE_ACTION, REBALANCE_ACTION_CLAIM)
                .put("queueName", queueName)
                .put("expectedOwner", expectedOwner);
        return vertx.eventBus().<Boolean>request(address, request)
                .map(message -> message.body() != null && message.body());
    }

    private Future<ExecutionResult> executePlan(QueueRebalancePlanner.Plan plan, JsonArray runningStates) {
        Future<ExecutionResult> chain = Future.succeededFuture(new ExecutionResult());
        for (QueueRebalancePlanner.Move move : plan.getMoves()) {
            chain = chain.compose(result -> executeMove(move, runningStates, result));
        }
        return chain;
    }

    private Future<ExecutionResult> executeMove(QueueRebalancePlanner.Move move, JsonArray runningStates, ExecutionResult result) {
        String queueName = move.getQueueName();
        if (!isQueueReadyInSnapshot(runningStates, move.getSourceConsumerId(), queueName)) {
            result.reasonsByQueue.put(queueName, SKIP_NOT_READY);
            return Future.succeededFuture(result);
        }

        String consumerKey = keyspaceHelper.getConsumersPrefix() + queueName;
        return redisService.get(consumerKey).recover(throwable -> {
            result.reasonsByQueue.put(queueName, SKIP_OWNER_LOOKUP_FAILED);
            return Future.succeededFuture();
        }).compose(currentOwnerResponse -> {
            if (result.reasonsByQueue.containsKey(queueName)) {
                return Future.succeededFuture(result);
            }
            String currentOwner = Objects.toString(currentOwnerResponse, null);
            if (!move.getSourceConsumerId().equals(currentOwner)) {
                result.reasonsByQueue.put(queueName, SKIP_OWNER_CHANGED);
                return Future.succeededFuture(result);
            }

            return requestTargetClaim(move.getTargetConsumerId(), queueName, move.getSourceConsumerId())
                    .compose(claimed -> {
                        if (!claimed) {
                            result.reasonsByQueue.put(queueName, SKIP_CLAIM_FAILED);
                            return Future.succeededFuture(result);
                        }
                        return requestSourceRelease(move.getSourceConsumerId(), queueName, move.getTargetConsumerId())
                                .compose(released -> {
                                    if (!released) {
                                        return recoverFailedHandoff(move, result, SKIP_RELEASE_FAILED);
                                    }
                                    result.executedMoves++;
                                    return Future.succeededFuture(result);
                                });
                    });
        });
    }

    private Future<ExecutionResult> recoverFailedHandoff(QueueRebalancePlanner.Move move, ExecutionResult result,
                                                         String skipReason) {
        String queueName = move.getQueueName();
        return requestTargetClaim(move.getSourceConsumerId(), queueName, move.getTargetConsumerId())
                .compose(reclaimed -> {
                    result.reasonsByQueue.put(queueName, skipReason);
                    return Future.succeededFuture(result);
                });
    }

    private PlannerInput buildPlannerInput(JsonArray runningStates, Set<String> queueScope) {
        Map<String, Integer> loadByConsumer = new LinkedHashMap<>();
        Map<String, List<String>> readyQueuesByConsumer = new LinkedHashMap<>();
        if (runningStates == null || runningStates.isEmpty() || queueScope.isEmpty()) {
            return new PlannerInput(loadByConsumer, readyQueuesByConsumer);
        }

        for (Object payloadEntry : runningStates) {
            if (!(payloadEntry instanceof JsonObject)) {
                continue;
            }
            JsonObject instancePayload = (JsonObject) payloadEntry;
            String consumerId = instancePayload.getString("consumerId");
            if (consumerId == null) {
                continue;
            }
            JsonObject queues = instancePayload.getJsonObject("queues", instancePayload);
            AtomicInteger ownedQueuesInScope = new AtomicInteger();
            List<String> readyQueues = new ArrayList<>();
            queues.forEach(entry -> {
                if (!(entry.getValue() instanceof JsonObject) || !queueScope.contains(entry.getKey())) {
                    return;
                }
                ownedQueuesInScope.incrementAndGet();
                JsonObject queueState = (JsonObject) entry.getValue();
                if (QueueState.READY.name().equals(queueState.getString("state"))) {
                    readyQueues.add(entry.getKey());
                }
            });
            loadByConsumer.put(consumerId, ownedQueuesInScope.get());
            readyQueuesByConsumer.put(consumerId, readyQueues);
        }
        return new PlannerInput(loadByConsumer, readyQueuesByConsumer);
    }

    private boolean isQueueReadyInSnapshot(JsonArray runningStates, String consumerId, String queueName) {
        if (runningStates == null) {
            return false;
        }
        for (Object payloadEntry : runningStates) {
            if (!(payloadEntry instanceof JsonObject)) {
                continue;
            }
            JsonObject instancePayload = (JsonObject) payloadEntry;
            if (!consumerId.equals(instancePayload.getString("consumerId"))) {
                continue;
            }
            JsonObject queueState = instancePayload.getJsonObject("queues", new JsonObject()).getJsonObject(queueName);
            return queueState != null && QueueState.READY.name().equals(queueState.getString("state"));
        }
        return false;
    }

    private Set<String> toQueueScope(Response response, Optional<Pattern> filterPattern) {
        Set<String> queueScope = new LinkedHashSet<>();
        if (response == null) {
            return queueScope;
        }
        for (Response entry : response) {
            String queueName = Objects.toString(entry, null);
            if (queueName == null) {
                continue;
            }
            if (filterPattern.isPresent() && !filterPattern.get().matcher(queueName).matches()) {
                continue;
            }
            queueScope.add(queueName);
        }
        return queueScope;
    }

    private JsonObject createSuccessReply(int plannedMoves, int executedMoves, Map<String, String> reasonsByQueue) {
        JsonObject reasons = new JsonObject();
        reasonsByQueue.forEach(reasons::put);
        return createOkReply().put(VALUE, new JsonObject()
                .put("plannedMoves", plannedMoves)
                .put("executedMoves", executedMoves)
                .put("skipped", reasonsByQueue.size())
                .put("reasonsByQueue", reasons));
    }

    private static class PlanningContext {
        private final Set<String> queueScope;
        private final JsonArray runningStates;

        private PlanningContext(Set<String> queueScope, JsonArray runningStates) {
            this.queueScope = queueScope;
            this.runningStates = runningStates;
        }
    }

    private static class PlannerInput {
        private final Map<String, Integer> loadByConsumer;
        private final Map<String, List<String>> readyQueuesByConsumer;

        private PlannerInput(Map<String, Integer> loadByConsumer, Map<String, List<String>> readyQueuesByConsumer) {
            this.loadByConsumer = loadByConsumer;
            this.readyQueuesByConsumer = readyQueuesByConsumer;
        }
    }

    private static class ExecutionResult {
        private final Map<String, String> reasonsByQueue = new LinkedHashMap<>();
        private int executedMoves;
    }
}
