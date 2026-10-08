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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Integration test for rebalancing a strongly uneven queue distribution.
 *
 * <p>The test deploys three real {@link RedisQues} verticles that share the same Redis
 * keyspace and event bus. It starts them with 100, 10, and 0 READY queues respectively,
 * invokes {@link RebalanceQueuesAction}, and verifies both persisted ownership in Redis
 * and the in-memory READY state maintained by each consumer.</p>
 */
public class RebalanceQueuesActionDistributionExtTest extends AbstractTestCase {

    // Queue handoffs update Redis and consumer-local state asynchronously, so allow up to three seconds to converge.
    private static final int REBALANCE_ATTEMPTS = 30;
    private static final int QUEUES_ON_A = 100;
    private static final int QUEUES_ON_B = 10;

    private final List<RedisQues> redisQuesVerticles = new ArrayList<>();

    @Rule
    public Timeout rule = Timeout.seconds(60);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();

        /*
         * All three verticles use the same configuration and processor address. This makes
         * them members of one RedisQues consumer group, allowing the rebalance action to
         * discover all three running states and transfer queues between their control endpoints.
         */
        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .micrometerMetricsEnabled(true)
                .micrometerPerQueueMetricsEnabled(true)
                .micrometerMetricsIdentifier("rebalance-distribution-ext")
                .refreshPeriod(2)
                .publishMetricsAddress("rebalance-distribution-ext-metrics")
                .metricStorageName("rebalance-distribution-ext-storage")
                .metricRefreshPeriod(2)
                .memoryUsageLimitPercent(80)
                .redisReadyCheckIntervalMs(2000)
                .build()
                .asJsonObject();

        // Wait until every verticle is deployed before seeding queues or invoking the action.
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
    public void rebalanceQueues_DistributesOneHundredTenQueuesAcrossThreeInstances(TestContext context) {
        // Keep this test independent from data left by other Redis-backed integration tests.
        flushAll();

        RedisQues instanceA = redisQuesVerticles.get(0);
        RedisQues instanceB = redisQuesVerticles.get(1);
        RedisQues instanceC = redisQuesVerticles.get(2);
        List<String> queueNames = new ArrayList<>();

        /*
         * Initial distribution:
         *   instance A: 100 queues
         *   instance B:  10 queues
         *   instance C:   0 queues
         *
         * There are 110 queues and three consumers, so the balanced targets are 37, 37,
         * and 36. The planner gives the two remainder queues to the consumers with the
         * highest initial loads, resulting in targets A=37, B=37, C=36.
         */
        seedReadyQueues(instanceA, queueNames, "rebalance-distribution-a-", QUEUES_ON_A);
        seedReadyQueues(instanceB, queueNames, "rebalance-distribution-b-", QUEUES_ON_B);

        /*
         * Read the consumer ownership keys through Jedis and verify the exact instance
         * identities, not only the sorted distribution, before any rebalance can occur.
         */
        Map<String, Integer> ownershipBeforeRebalance = ownershipCounts(queueNames);
        context.assertEquals(100, ownershipBeforeRebalance.get(instanceA.getUid()).intValue());
        context.assertEquals(10, ownershipBeforeRebalance.get(instanceB.getUid()).intValue());
        context.assertEquals(0, ownershipBeforeRebalance.get(instanceC.getUid()).intValue());
        context.assertEquals(List.of(0, 10, 100), sortedReadyCounts(queueNames));

        Async async = context.async();
        vertx.eventBus().<JsonObject>request(
                keyspaceHelper.getAddress(),
                RedisquesAPI.buildRebalanceQueuesOperation("rebalance-distribution-.*", false, 100)
        ).onComplete(context.asyncAssertSuccess(message -> {
            JsonObject body = message.body();
            JsonObject value = body.getJsonObject("value");
            context.assertEquals("ok", body.getString("status"));

            /*
             * A must release 63 queues to reach its target of 37. B receives 27 and C
             * receives 36, so all 63 planned handoffs fit within maxMovesPerRun=100.
             */
            context.assertEquals(63, value.getInteger("plannedMoves").intValue());
            context.assertEquals(63, value.getInteger("executedMoves").intValue());
            context.assertEquals(0, value.getInteger("skipped").intValue());
            context.assertTrue(value.getJsonObject("reasonsByQueue").isEmpty());

            /*
             * A successful action reply means the handoff protocol completed. Poll the
             * observable Redis and local consumer states as well, ensuring the system
             * actually converges to the planned distribution.
             */
            assertEventuallyDistributed(context, async, queueNames,
                    instanceA, instanceB, instanceC, REBALANCE_ATTEMPTS);
        }));
    }

    private void seedReadyQueues(RedisQues redisQues, List<String> queueNames, String prefix, int count) {
        for (int i = 1; i <= count; i++) {
            String queueName = prefix + i;
            long now = System.currentTimeMillis();
            queueNames.add(queueName);

            /*
             * Reproduce all state used by the rebalance action:
             * 1. The sorted-set entry puts the queue in the action's queue scope.
             * 2. The consumer key records the current owner in Redis.
             * 3. The local QueueProcessingState reports that the owner can release the queue.
             */
            jedis.zadd(keyspaceHelper.getQueuesKey(), now, queueName);
            jedis.set(getConsumersRedisKeyPrefix() + queueName, redisQues.getUid());
            redisQues.getQueueConsumerRunner().getMyQueues()
                    .put(queueName, new QueueProcessingState(QueueState.READY, now));
        }
    }

    private void assertEventuallyDistributed(TestContext context, Async async, List<String> queueNames,
                                             RedisQues instanceA, RedisQues instanceB, RedisQues instanceC,
                                             int attemptsRemaining) {
        /*
         * Verify both sources of truth. Redis ownership alone would not prove that the
         * target consumers registered the queues as READY, while local state alone would
         * not prove that the ownership keys were transferred correctly.
         */
        Map<String, Integer> ownershipCounts = ownershipCounts(queueNames);
        List<Integer> readyCounts = sortedReadyCounts(queueNames);
        boolean ownershipIsDistributed =
                ownershipCounts.get(instanceA.getUid()) == 37
                        && ownershipCounts.get(instanceB.getUid()) == 37
                        && ownershipCounts.get(instanceC.getUid()) == 36;
        if (ownershipIsDistributed && readyCounts.equals(List.of(36, 37, 37))) {
            async.complete();
            return;
        }

        if (attemptsRemaining <= 0) {
            context.fail("Expected ownership and READY counts [36, 37, 37] but found ownership="
                    + ownershipCounts + " ready=" + readyCounts);
            return;
        }

        // Retry briefly because event-bus handoffs and consumer state updates are asynchronous.
        vertx.setTimer(100L, ignored ->
                assertEventuallyDistributed(context, async, queueNames,
                        instanceA, instanceB, instanceC, attemptsRemaining - 1));
    }

    private Map<String, Integer> ownershipCounts(List<String> queueNames) {
        // Count the authoritative Jedis ownership keys while preserving each consumer UID.
        Map<String, Integer> counts = emptyCountsByUid();
        for (String queueName : queueNames) {
            String owner = jedis.get(getConsumersRedisKeyPrefix() + queueName);
            if (owner != null) {
                counts.computeIfPresent(owner, (key, value) -> value + 1);
            }
        }
        return counts;
    }

    private List<Integer> sortedReadyCounts(List<String> queueNames) {
        // Count queues that each running consumer currently exposes in its local READY state.
        Map<String, Integer> counts = emptyCountsByUid();
        for (RedisQues redisQues : redisQuesVerticles) {
            redisQues.getQueueConsumerRunner().getMyQueues().forEach((queueName, state) -> {
                if (queueNames.contains(queueName) && state.getState() == QueueState.READY) {
                    counts.computeIfPresent(redisQues.getUid(), (key, value) -> value + 1);
                }
            });
        }
        return sortedCounts(counts);
    }

    private Map<String, Integer> emptyCountsByUid() {
        // Include consumers with no queues so instance C contributes an explicit zero before rebalancing.
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (RedisQues redisQues : redisQuesVerticles) {
            counts.put(redisQues.getUid(), 0);
        }
        return counts;
    }

    private List<Integer> sortedCounts(Map<String, Integer> counts) {
        // Consumer UIDs are generated dynamically, so compare the distribution independent of instance identity.
        List<Integer> sortedCounts = new ArrayList<>(counts.values());
        Collections.sort(sortedCounts);
        return sortedCounts;
    }
}
