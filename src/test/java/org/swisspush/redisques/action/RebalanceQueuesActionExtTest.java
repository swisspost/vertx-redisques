package org.swisspush.redisques.action;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.queue.QueueProcessingState;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RebalanceQueuesActionExtTest extends AbstractTestCase {

    private final List<RedisQues> redisQuesVerticles = new ArrayList<>();

    @Rule
    public Timeout rule = Timeout.seconds(60);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();

        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .micrometerMetricsEnabled(true)
                .micrometerPerQueueMetricsEnabled(true)
                .micrometerMetricsIdentifier("rebalance-ext")
                .refreshPeriod(2)
                .publishMetricsAddress("rebalance-ext-metrics")
                .metricStorageName("rebalance-ext-storage")
                .metricRefreshPeriod(2)
                .memoryUsageLimitPercent(80)
                .redisReadyCheckIntervalMs(2000)
                .build()
                .asJsonObject();

        Async async = context.async(3);
        for (int i = 0; i < 3; i++) {
            MeterRegistry meterRegistry = new SimpleMeterRegistry();
            RedisQues redisQues = RedisQues.builder()
                    .withMemoryUsageProvider(new TestMemoryUsageProvider(Optional.of(50)))
                    .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(vertx, config))
                    .withMeterRegistry(meterRegistry)
                    .build();
            redisQues.disableMigrationTool();
            redisQuesVerticles.add(redisQues);
            vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(id -> {
                if (jedis == null) {
                    jedis = new Jedis("localhost", 6379, 5000);
                    keyspaceHelper = redisQues.getKeyspaceHelper();
                }
                async.countDown();
            }));
        }
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void rebalanceQueues_MovesReadyQueuesTowardEvenDistribution(TestContext context) {
        flushAll();

        RedisQues donor = redisQuesVerticles.get(0);
        RedisQues receiverA = redisQuesVerticles.get(1);
        RedisQues receiverB = redisQuesVerticles.get(2);
        List<String> queueNames = List.of(
                "rebalance-ready-q1",
                "rebalance-ready-q2",
                "rebalance-ready-q3",
                "rebalance-ready-q4",
                "rebalance-ready-q5",
                "rebalance-ready-q6",
                "rebalance-ready-q7"
        );

        seedReadyQueues(donor, queueNames.subList(0, 5));
        seedReadyQueues(receiverA, queueNames.subList(5, 6));
        seedReadyQueues(receiverB, queueNames.subList(6, 7));

        Async async = context.async();
        vertx.eventBus().<JsonObject>request(
                keyspaceHelper.getAddress(),
                RedisquesAPI.buildRebalanceQueuesOperation("rebalance-ready-.*", false, 10)
        ).onComplete(context.asyncAssertSuccess(message -> {
            JsonObject body = message.body();
            JsonObject value = body.getJsonObject("value");
            context.assertEquals("ok", body.getString("status"));
            context.assertTrue(value.getInteger("plannedMoves") >= 2);
            context.assertTrue(value.getInteger("executedMoves") >= 2);
            context.assertTrue(value.getInteger("executedMoves") <= value.getInteger("plannedMoves"));
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertTrue(value.getJsonObject("reasonsByQueue").isEmpty());
            assertEventuallyBalanced(context, async, queueNames, 10);
        }));
    }

    private void assertEventuallyBalanced(TestContext context, Async async, List<String> queueNames, int attemptsRemaining) {
        Map<String, Integer> ownershipCounts = ownershipCounts(queueNames);
        List<Integer> sortedOwnershipCounts = ownershipCounts.values().stream().sorted().collect(Collectors.toList());
        if (sortedOwnershipCounts.equals(List.of(2, 2, 3))) {
            Map<String, Integer> readyCounts = readyCounts(queueNames);
            List<Integer> sortedReadyCounts = readyCounts.values().stream().sorted().collect(Collectors.toList());
            if (sortedReadyCounts.equals(List.of(2, 2, 3))) {
                async.complete();
                return;
            }
        }

        if (attemptsRemaining <= 0) {
            context.fail("Expected balanced ownership and READY distribution but found ownership=" + ownershipCounts
                    + " ready=" + readyCounts(queueNames));
            return;
        }

        vertx.setTimer(100L, ignored -> assertEventuallyBalanced(context, async, queueNames, attemptsRemaining - 1));
    }

    private void seedReadyQueues(RedisQues redisQues, List<String> queueNames) {
        for (String queueName : queueNames) {
            long now = System.currentTimeMillis();
            jedis.zadd(keyspaceHelper.getQueuesKey(), now, queueName);
            jedis.set(getConsumersRedisKeyPrefix() + queueName, redisQues.getUid());
            redisQues.getQueueConsumerRunner().getMyQueues().put(queueName, new QueueProcessingState(QueueState.READY, now));
        }
    }

    private Map<String, Integer> ownershipCounts(List<String> queueNames) {
        Map<String, Integer> counts = emptyCountsByUid();
        for (String queueName : queueNames) {
            String owner = jedis.get(getConsumersRedisKeyPrefix() + queueName);
            if (owner != null) {
                counts.computeIfPresent(owner, (key, value) -> value + 1);
            }
        }
        return sortByValue(counts);
    }

    private Map<String, Integer> readyCounts(List<String> queueNames) {
        Map<String, Integer> counts = emptyCountsByUid();
        for (RedisQues redisQues : redisQuesVerticles) {
            redisQues.getQueueConsumerRunner().getMyQueues().forEach((queueName, state) -> {
                if (queueNames.contains(queueName) && state.getState() == QueueState.READY) {
                    counts.compute(redisQues.getUid(), (key, value) -> value == null ? 1 : value + 1);
                }
            });
        }
        return sortByValue(counts);
    }

    private Map<String, Integer> emptyCountsByUid() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (RedisQues redisQues : redisQuesVerticles) {
            counts.put(redisQues.getUid(), 0);
        }
        return counts;
    }

    private Map<String, Integer> sortByValue(Map<String, Integer> counts) {
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(counts.entrySet());
        entries.sort(Map.Entry.comparingByValue(Comparator.naturalOrder()));
        Map<String, Integer> sorted = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : entries) {
            sorted.put(entry.getKey(), entry.getValue());
        }
        return sorted;
    }
}
