package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
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
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
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

        action = new TestableRebalanceQueuesAction(vertx, redisService, keyspaceHelper, redisquesConfigurationProvider);
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
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B"), action.protocolSteps);
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
            context.assertEquals(List.of("claim:B:q3:A", "release:A:q3:B"), action.protocolSteps);
            verify(redisService, times(1)).get("consumer:q3");
            verify(redisService, never()).get("consumer:q1");
            verify(redisService, never()).get("consumer:q2");
            verify(redisService, never()).setNxPx(anyString(), anyString(), eq(false), anyLong());
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
            context.assertEquals(List.of("claim:B:q1:A", "release:A:q1:B", "claim:A:q1:B"), action.protocolSteps);
            context.assertEquals("C", action.currentOwners.getString("q1"));
            context.assertTrue(action.localReadyQueues.contains("A:q1"));
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
            context.assertEquals(List.of("claim:C:q2:A", "release:A:q2:C"), action.protocolSteps);
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
        private final Map<String, String> releaseFailures = new LinkedHashMap<>();
        private final Map<String, String> claimFailures = new LinkedHashMap<>();
        private final JsonObject concurrentOwnersAfterReleaseFailure = new JsonObject();
        private final List<String> localReadyQueues = new ArrayList<>();
        private final List<String> protocolSteps = new ArrayList<>();

        private TestableRebalanceQueuesAction(Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
                                              RedisquesConfigurationProvider redisquesConfigurationProvider) {
            super(vertx, redisService, keyspaceHelper, mock(QueueConfigurationProvider.class),
                    redisquesConfigurationProvider, RedisQuesExceptionFactory.newWastefulExceptionFactory(),
                    mock(QueueStatisticsCollector.class), mock(Logger.class));
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
            if (releaseFailures.containsKey(configuredKey)) {
                return Future.failedFuture(releaseFailures.get(configuredKey));
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
    }
}
