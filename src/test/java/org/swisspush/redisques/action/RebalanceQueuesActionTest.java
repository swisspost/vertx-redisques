package org.swisspush.redisques.action;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.metrics.RebalanceMetrics;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesAPI;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class RebalanceQueuesActionTest {

    private Vertx vertx;
    private TestableRebalanceQueuesAction action;
    private RedisService redisService;
    private KeyspaceHelper keyspaceHelper;
    private RedisquesConfigurationProvider redisquesConfigurationProvider;
    private SimpleMeterRegistry meterRegistry;

    @Before
    public void setup() {
        vertx = Vertx.vertx();
        redisService = mock(RedisService.class);
        keyspaceHelper = mock(KeyspaceHelper.class);
        redisquesConfigurationProvider = mock(RedisquesConfigurationProvider.class);

        RedisquesConfiguration configuration = mock(RedisquesConfiguration.class);
        when(configuration.getConsumerLockMultiplier()).thenReturn(2);
        when(configuration.getRefreshPeriod()).thenReturn(3);
        when(redisquesConfigurationProvider.configuration()).thenReturn(configuration);

        when(keyspaceHelper.getQueuesKey()).thenReturn("queues-key");
        when(keyspaceHelper.getConsumersPrefix()).thenReturn("consumer:");
        when(keyspaceHelper.getAddress()).thenReturn("redisques");

        meterRegistry = new SimpleMeterRegistry();
        action = new TestableRebalanceQueuesAction(vertx, redisService, keyspaceHelper, redisquesConfigurationProvider,
                new RebalanceMetrics(meterRegistry, "foo"));
    }

    private double moveCount(String result) {
        io.micrometer.core.instrument.Counter counter = meterRegistry.find(MetricMeter.REBALANCE_MOVE.getId())
                .tag(MetricTags.IDENTIFIER.getId(), "foo").tag(MetricTags.RESULT.getId(), result).counter();
        return counter == null ? 0 : counter.count();
    }

    @Test
    public void execute_ReportsMoveResultMetrics(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");
        action.claimReplies.put("B:q1:A", false);

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            context.assertEquals(1.0, moveCount("claim-failed"));
            context.assertEquals(0.0, moveCount(RebalanceMetrics.RESULT_EXECUTED));
        });
    }

    @Test
    public void execute_ReportsExecutedMoveMetric(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply ->
                context.assertEquals(1.0, moveCount(RebalanceMetrics.RESULT_EXECUTED)));
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void execute_DryRunReturnsPlanWithoutExecuting(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", true, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(1, value.getInteger("plannedMoves").intValue());
            context.assertEquals(0, value.getInteger("executedMoves").intValue());
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertTrue(value.getJsonObject("reasonsByQueue").isEmpty());
            verify(redisService, never()).get("consumer:q1");
            verify(redisService, never()).setNxPx(anyString(), anyString(), eq(false), anyLong());
        });
    }

    @Test
    public void execute_InvalidMaxMovesPerRunReturnsBadInput(TestContext context) {
        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 0), reply -> {
            context.assertEquals("error", reply.getString("status"));
            context.assertEquals("bad input", reply.getString("errorType"));
            context.assertEquals("maxMovesPerRun must be between 1 and 100", reply.getString("message"));
            verify(redisService, never()).zrangebyscore(anyString(), anyString(), anyString());
        });
    }

    @Test
    public void execute_ReturnsErrorWhenRunningStatesAreIncomplete(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY)));
        action.aliveConsumerCount = 2;

        executeAndAssertRaw(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            context.assertTrue(reply instanceof ReplyException);
            context.assertEquals(2, action.lastRequestedExpectedReplies);
            verify(redisService, never()).get("consumer:q1");
        });
    }

    @Test
    public void execute_MaxMovesPerRunAboveHardLimitIsCapped(TestContext context) {
        List<String> queueNames = new ArrayList<>();
        for (int i = 1; i <= 201; i++) {
            queueNames.add("q" + i);
        }

        Response queuesResponse = responseList(queueNames.toArray(new String[0]));
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueStates(queueNames, QueueState.READY)))
                .add(consumerState("B"))
                .add(consumerState("C"));

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", true, 101), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(100, value.getInteger("plannedMoves").intValue());
            context.assertEquals(0, value.getInteger("executedMoves").intValue());
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertTrue(value.getJsonObject("reasonsByQueue").isEmpty());
            verify(redisService, never()).get(anyString());
        });
    }

    @Test
    public void execute_PerformsPlannedMoveWhenOwnershipAndStateStillMatch(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(1, value.getInteger("plannedMoves").intValue());
            context.assertEquals(1, value.getInteger("executedMoves").intValue());
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertTrue(value.getJsonObject("reasonsByQueue").isEmpty());
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B", "activate:B:q1"), action.protocolSteps);
            context.assertEquals("B", action.currentOwners.getString("q1"));
            context.assertFalse(action.localReadyQueues.contains("A:q1"));
            context.assertTrue(action.localReadyQueues.contains("B:q1"));
            verify(redisService, times(1)).get("consumer:q1");
            verify(redisService, never()).setNxPx("consumer:q1", "B", false, 6000L);
        });
    }

    @Test
    public void execute_CountsAllOwnedQueuesForLoadButMovesOnlyReadyQueues(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5", "q6");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q3")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.CONSUMING), queueState("q2", QueueState.CONSUMING),
                        queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q3", "A");
        action.localReadyQueues.add("A:q3");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 1), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(1, value.getInteger("plannedMoves").intValue());
            context.assertEquals(1, value.getInteger("executedMoves").intValue());
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertTrue(value.getJsonObject("reasonsByQueue").isEmpty());
            context.assertEquals(List.of("claim:B:q3:A", "release:A:q3:B", "activate:B:q3"), action.protocolSteps);
            verify(redisService, times(1)).get("consumer:q3");
            verify(redisService, never()).get("consumer:q1");
            verify(redisService, never()).get("consumer:q2");
            verify(redisService, never()).setNxPx(anyString(), anyString(), eq(false), anyLong());
        });
    }

    @Test
    public void execute_DoesNotPlanQueuesWithPendingRebalanceClaims(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q2")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", pendingQueueState("q1", "previous-owner"), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q2", "A");
        action.localReadyQueues.add("A:q2");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", true, 1), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals(1, value.getInteger("plannedMoves").intValue());
            context.assertTrue(action.protocolSteps.isEmpty());
            verify(redisService, never()).get("consumer:q1");
        });
    }

    @Test
    public void execute_ClaimFailureSkipsReleaseAndNotify(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");
        action.claimReplies.put("B:q1:A", false);

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            JsonObject reasonsByQueue = value.getJsonObject("reasonsByQueue");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(1, value.getInteger("plannedMoves").intValue());
            context.assertEquals(0, value.getInteger("executedMoves").intValue());
            context.assertEquals(1, value.getInteger("skipped").intValue());
            context.assertEquals("claim-failed", reasonsByQueue.getString("q1"));
            context.assertEquals(List.of("claim:B:q1:A"), action.protocolSteps);
            context.assertEquals("A", action.currentOwners.getString("q1"));
        });
    }

    @Test
    public void execute_ReleaseFailureRollbackKeepsFreshConcurrentOwner(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");
        action.releaseReplies.put("A:q1:B", false);
        action.concurrentOwnersAfterReleaseFailure.put("q1", "C");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            JsonObject reasonsByQueue = value.getJsonObject("reasonsByQueue");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(1, value.getInteger("plannedMoves").intValue());
            context.assertEquals(0, value.getInteger("executedMoves").intValue());
            context.assertEquals(1, value.getInteger("skipped").intValue());
            context.assertEquals("release-failed", reasonsByQueue.getString("q1"));
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B", "abandon:B:q1:A"), action.protocolSteps);
            context.assertEquals("C", action.currentOwners.getString("q1"));
            context.assertTrue(action.localReadyQueues.contains("A:q1"));
        });
    }

    @Test
    public void execute_ExplicitSourceReleaseFailureRetriesBeforeCompletingHandoff(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");
        action.releaseFailures.put("A:q1:B", new ArrayList<>(List.of(
                new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 0, "source release failed"))));

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(1, value.getInteger("executedMoves").intValue());
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B", "release:A:q1:B", "activate:B:q1"), action.protocolSteps);
            context.assertEquals("B", action.currentOwners.getString("q1"));
            context.assertTrue(action.localReadyQueues.contains("B:q1"));
        });
    }

    @Test
    public void execute_RetriesAmbiguousSourceReleaseAndCompletesHandoff(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");
        action.releaseFailures.put("A:q1:B", new ArrayList<>(List.of(
                new ReplyException(ReplyFailure.TIMEOUT, 0, "release reply timed out"))));

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(1, value.getInteger("executedMoves").intValue());
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B", "release:A:q1:B", "activate:B:q1"), action.protocolSteps);
            context.assertEquals("B", action.currentOwners.getString("q1"));
            context.assertTrue(action.localReadyQueues.contains("B:q1"));
            context.assertFalse(action.localReadyQueues.contains("A:q1"));
        });
    }

    @Test
    public void execute_ReconcilesPendingClaimOnLaterRequest(TestContext context) {
        Response queuesResponse = responseList("q1");
        Response retryQueuesResponse = responseList("q1");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse), Future.succeededFuture(retryQueuesResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A"))
                .add(consumerState("B", pendingQueueState("q1", "A")))
                .add(consumerState("C"));
        action.currentOwners.put("q1", "B");
        action.localReadyQueues.add("A:q1");
        action.localReadyQueues.add("B:q1");
        action.releaseFailures.put("A:q1:B", new ArrayList<>(List.of(
                new ReplyException(ReplyFailure.TIMEOUT, 0, "first release timed out"))));

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 1), firstReply -> {
            JsonObject firstValue = firstReply.getJsonObject("value");
            context.assertEquals(1, firstValue.getInteger("plannedMoves").intValue());
            context.assertEquals(0, firstValue.getInteger("executedMoves").intValue());
            context.assertEquals("release-failed", firstValue.getJsonObject("reasonsByQueue").getString("q1"));
            context.assertEquals("B", action.currentOwners.getString("q1"));
            context.assertTrue(action.localReadyQueues.contains("B:q1"));
            action.runningStates = new JsonArray()
                    .add(consumerState("A"))
                    .add(consumerState("B", pendingQueueState("q1", "A")))
                    .add(consumerState("C"));

            executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 1), secondReply -> {
                JsonObject secondValue = secondReply.getJsonObject("value");
                context.assertEquals(List.of("release:A:q1:B", "release:A:q1:B", "activate:B:q1"), action.protocolSteps);
                context.assertEquals(1, secondValue.getInteger("plannedMoves").intValue());
                context.assertEquals(1, secondValue.getInteger("executedMoves").intValue());
                context.assertEquals(0, secondValue.getInteger("skipped").intValue());
                context.assertTrue(action.localReadyQueues.contains("B:q1"));
                context.assertFalse(action.localReadyQueues.contains("A:q1"));
            });
        });
    }

    @Test
    public void execute_ActivateFailureIsSkippedButDoesNotAbortRemainingMoves(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q6", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        when(redisService.get("consumer:q2")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY),
                        queueState("q3", QueueState.READY), queueState("q6", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.currentOwners.put("q2", "A");
        action.localReadyQueues.add("A:q1");
        action.localReadyQueues.add("A:q2");
        action.activateFailures.put("B:q1", "activate infra failed");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            JsonObject reasonsByQueue = value.getJsonObject("reasonsByQueue");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(2, value.getInteger("plannedMoves").intValue());
            // The activate failure on q1 must not prevent the still-pending q2 move from executing.
            context.assertEquals(1, value.getInteger("executedMoves").intValue());
            context.assertEquals(1, value.getInteger("skipped").intValue());
            context.assertEquals("activate-failed", reasonsByQueue.getString("q1"));
            context.assertNull(reasonsByQueue.getString("q2"));
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B", "activate:B:q1",
                    "claim:C:q2:A", "release:A:q2:C", "activate:C:q2"), action.protocolSteps);
            // Ownership already transferred to the target and must not be rolled back on activate failure.
            context.assertEquals("B", action.currentOwners.getString("q1"));
            context.assertTrue(action.localReadyQueues.contains("B:q1"));
            context.assertFalse(action.localReadyQueues.contains("A:q1"));
        });
    }

    @Test
    public void execute_AbandonFailureIsSkippedButDoesNotAbortRemainingMoves(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q6", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        when(redisService.get("consumer:q2")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY),
                        queueState("q3", QueueState.READY), queueState("q6", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.currentOwners.put("q2", "A");
        action.localReadyQueues.add("A:q1");
        action.localReadyQueues.add("A:q2");
        action.releaseReplies.put("A:q1:B", false);
        action.abandonFailures.put("B:q1", "abandon infra failed");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(2, value.getInteger("plannedMoves").intValue());
            context.assertEquals(1, value.getInteger("executedMoves").intValue());
            context.assertEquals("release-failed", value.getJsonObject("reasonsByQueue").getString("q1"));
            context.assertNull(value.getJsonObject("reasonsByQueue").getString("q2"));
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B", "abandon:B:q1:A",
                    "claim:C:q2:A", "release:A:q2:C", "activate:C:q2"), action.protocolSteps);
        });
    }

    @Test
    public void execute_ClaimInfrastructureFailureReturnsError(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5");
        Response ownerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY), queueState("q3", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q1", "A");
        action.localReadyQueues.add("A:q1");
        action.claimFailures.put("B:q1:A", "claim infra failed");

        executeAndAssertRaw(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            context.assertTrue(reply instanceof ReplyException);
            context.assertEquals(List.of("claim:B:q1:A"), action.protocolSteps);
            context.assertEquals("A", action.currentOwners.getString("q1"));
            context.assertTrue(action.localReadyQueues.contains("A:q1"));
        });
    }

    @Test
    public void execute_SkipsChangedOwnerButContinuesWithRemainingReadyMove(TestContext context) {
        Response queuesResponse = responseList("q1", "q2", "q3", "q4", "q5", "q6");
        Response ownerResponse = stringResponse("other-owner");
        Response secondOwnerResponse = stringResponse("A");
        when(redisService.zrangebyscore(eq("queues-key"), anyString(), eq("+inf")))
                .thenReturn(Future.succeededFuture(queuesResponse));
        when(redisService.get("consumer:q1")).thenReturn(Future.succeededFuture(ownerResponse));
        when(redisService.get("consumer:q2")).thenReturn(Future.succeededFuture(secondOwnerResponse));
        action.runningStates = new JsonArray()
                .add(consumerState("A", queueState("q1", QueueState.READY), queueState("q2", QueueState.READY),
                        queueState("q3", QueueState.READY), queueState("q6", QueueState.READY)))
                .add(consumerState("B", queueState("q4", QueueState.READY)))
                .add(consumerState("C", queueState("q5", QueueState.READY)));
        action.currentOwners.put("q2", "A");
        action.localReadyQueues.add("A:q2");

        executeAndAssert(context, RedisquesAPI.buildRebalanceQueuesOperation(".*", false, 5), reply -> {
            JsonObject value = reply.getJsonObject("value");
            JsonObject reasonsByQueue = value.getJsonObject("reasonsByQueue");
            context.assertEquals("ok", reply.getString("status"));
            context.assertEquals(2, value.getInteger("plannedMoves").intValue());
            context.assertEquals(1, value.getInteger("executedMoves").intValue());
            context.assertEquals(1, value.getInteger("skipped").intValue());
            context.assertEquals("owner-changed", reasonsByQueue.getString("q1"));
            context.assertNull(reasonsByQueue.getString("q2"));
            context.assertEquals(List.of("claim:C:q2:A", "release:A:q2:C", "activate:C:q2"), action.protocolSteps);
            verify(redisService, never()).setNxPx(anyString(), anyString(), eq(false), anyLong());
        });
    }

    private void executeAndAssert(TestContext context, JsonObject body, java.util.function.Consumer<JsonObject> assertions) {
        executeAndAssertRaw(context, body, reply -> assertions.accept((JsonObject) reply));
    }

    private void executeAndAssertRaw(TestContext context, JsonObject body, java.util.function.Consumer<Object> assertions) {
        Async async = context.async();
        @SuppressWarnings("unchecked")
        Message<JsonObject> message = Mockito.mock(Message.class);
        when(message.body()).thenReturn(body);
        doAnswer(invocation -> {
            assertions.accept(invocation.getArgument(0));
            async.complete();
            return null;
        }).when(message).reply(Mockito.any());
        action.execute(message);
    }

    private static JsonObject consumerState(String consumerId, JsonObject... queueStates) {
        JsonObject queues = new JsonObject();
        for (JsonObject queueState : queueStates) {
            queues.put(queueState.getString("name"), queueState.getJsonObject("value"));
        }
        return new JsonObject().put("consumerId", consumerId).put("queues", queues);
    }

    private static JsonObject consumerState(String consumerId) {
        return new JsonObject().put("consumerId", consumerId).put("queues", new JsonObject());
    }

    private static JsonObject[] queueStates(List<String> queueNames, QueueState state) {
        JsonObject[] queueStates = new JsonObject[queueNames.size()];
        for (int i = 0; i < queueNames.size(); i++) {
            queueStates[i] = queueState(queueNames.get(i), state);
        }
        return queueStates;
    }

    private static JsonObject queueState(String queueName, QueueState state) {
        return new JsonObject().put("name", queueName).put("value", new JsonObject().put("state", state.name()));
    }

    private static JsonObject pendingQueueState(String queueName, String previousOwner) {
        return new JsonObject().put("name", queueName)
                .put("value", new JsonObject().put("state", QueueState.READY.name())
                        .put("rebalancePending", true).put("rebalancePreviousOwner", previousOwner));
    }

    private static Response responseList(String... values) {
        Response response = mock(Response.class);
        List<Response> items = new ArrayList<>();
        for (String value : values) {
            items.add(stringResponse(value));
        }
        when(response.iterator()).thenReturn(items.iterator());
        when(response.stream()).thenReturn(items.stream());
        when(response.size()).thenReturn(items.size());
        for (int i = 0; i < items.size(); i++) {
            when(response.get(i)).thenReturn(items.get(i));
        }
        return response;
    }

    private static Response stringResponse(String value) {
        Response response = mock(Response.class);
        when(response.toString()).thenReturn(value);
        return response;
    }

    private static class TestableRebalanceQueuesAction extends RebalanceQueuesAction {
        private JsonArray runningStates = new JsonArray();
        private Integer aliveConsumerCount;
        private int lastRequestedExpectedReplies = -1;
        private final JsonObject currentOwners = new JsonObject();
        private final JsonObject releaseReplies = new JsonObject();
        private final JsonObject claimReplies = new JsonObject();
        private final Map<String, List<Throwable>> releaseFailures = new LinkedHashMap<>();
        private final Map<String, String> claimFailures = new LinkedHashMap<>();
        private final Map<String, String> activateFailures = new LinkedHashMap<>();
        private final Map<String, String> abandonFailures = new LinkedHashMap<>();
        private final JsonObject concurrentOwnersAfterReleaseFailure = new JsonObject();
        private final List<String> localReadyQueues = new ArrayList<>();
        private final List<String> protocolSteps = new ArrayList<>();

        private TestableRebalanceQueuesAction(Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
                                              RedisquesConfigurationProvider redisquesConfigurationProvider,
                                              RebalanceMetrics rebalanceMetrics) {
            super(vertx, redisService, keyspaceHelper, mock(QueueConfigurationProvider.class),
                    redisquesConfigurationProvider, RedisQuesExceptionFactory.newWastefulExceptionFactory(),
                    mock(QueueStatisticsCollector.class), mock(Logger.class), rebalanceMetrics);
        }

        @Override
        protected Future<Integer> fetchExpectedAliveConsumerCount() {
            return Future.succeededFuture(aliveConsumerCount != null ? aliveConsumerCount : runningStates.size());
        }

        @Override
        protected Future<JsonArray> fetchRunningStates(int expectedReplies) {
            lastRequestedExpectedReplies = expectedReplies;
            return Future.succeededFuture(runningStates);
        }

        @Override
        protected Future<Boolean> requestSourceRelease(String sourceConsumerId, String queueName, String expectedOwner) {
            protocolSteps.add("release:" + sourceConsumerId + ":" + queueName + ":" + expectedOwner);
            String configuredKey = sourceConsumerId + ":" + queueName + ":" + expectedOwner;
            List<Throwable> failures = releaseFailures.get(configuredKey);
            if (failures != null && !failures.isEmpty()) {
                return Future.failedFuture(failures.remove(0));
            }
            if (!localReadyQueues.contains(sourceConsumerId + ":" + queueName)) {
                return Future.succeededFuture(false);
            }
            if (releaseReplies.containsKey(configuredKey) && !Boolean.TRUE.equals(releaseReplies.getBoolean(configuredKey))) {
                String concurrentOwner = concurrentOwnersAfterReleaseFailure.getString(queueName);
                if (concurrentOwner != null) {
                    currentOwners.put(queueName, concurrentOwner);
                }
                return Future.succeededFuture(false);
            }
            if (!expectedOwner.equals(currentOwners.getString(queueName))) {
                return Future.succeededFuture(false);
            }
            localReadyQueues.remove(sourceConsumerId + ":" + queueName);
            return Future.succeededFuture(true);
        }

        @Override
        protected Future<Boolean> requestTargetClaim(String targetConsumerId, String queueName, String expectedOwner) {
            protocolSteps.add("claim:" + targetConsumerId + ":" + queueName + ":" + expectedOwner);
            String configuredKey = targetConsumerId + ":" + queueName + ":" + expectedOwner;
            if (claimFailures.containsKey(configuredKey)) {
                return Future.failedFuture(claimFailures.get(configuredKey));
            }
            if (claimReplies.containsKey(configuredKey)) {
                Boolean reply = claimReplies.getBoolean(configuredKey);
                if (!Boolean.TRUE.equals(reply)) {
                    return Future.succeededFuture(false);
                }
            }
            if (!expectedOwner.equals(currentOwners.getString(queueName))) {
                return Future.succeededFuture(false);
            }
            currentOwners.put(queueName, targetConsumerId);
            localReadyQueues.add(targetConsumerId + ":" + queueName);
            return Future.succeededFuture(true);
        }

        @Override
        protected Future<Boolean> requestTargetActivate(String targetConsumerId, String queueName) {
            protocolSteps.add("activate:" + targetConsumerId + ":" + queueName);
            String configuredKey = targetConsumerId + ":" + queueName;
            if (activateFailures.containsKey(configuredKey)) {
                return Future.failedFuture(activateFailures.get(configuredKey));
            }
            return Future.succeededFuture(true);
        }

        @Override
        protected Future<Boolean> requestTargetAbandon(String targetConsumerId, String queueName, String rollbackOwner) {
            protocolSteps.add("abandon:" + targetConsumerId + ":" + queueName + ":" + rollbackOwner);
            if (abandonFailures.containsKey(targetConsumerId + ":" + queueName)) {
                return Future.failedFuture(abandonFailures.get(targetConsumerId + ":" + queueName));
            }
            if (!targetConsumerId.equals(currentOwners.getString(queueName))) {
                return Future.succeededFuture(false);
            }
            currentOwners.put(queueName, rollbackOwner);
            localReadyQueues.remove(targetConsumerId + ":" + queueName);
            return Future.succeededFuture(true);
        }
    }
}
