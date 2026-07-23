package org.swisspush.redisques.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.action.AbstractQueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.QueueProcessingState;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_NAME;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_SIZE;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUES;

/**
 * Tests for {@link RedisQuesTimer} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class QueueStatisticsCollectorTest extends AbstractTestCase {
    private RedisQues redisQues;
    private Vertx vertx;
    private QueueStatisticsCollector queueStatisticsCollector;
    private TestMemoryUsageProvider memoryUsageProvider;
    private final String metricsIdentifier = "foo";
    protected AbstractQueueAction action;
    protected RedisQuesExceptionFactory exceptionFactory;
    @Rule
    public Timeout rule = Timeout.seconds(50);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();
        Async async = context.async();
        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .micrometerMetricsEnabled(true)
                .micrometerPerQueueMetricsEnabled(true)
                .micrometerMetricsIdentifier(metricsIdentifier)
                .refreshPeriod(2)
                .publishMetricsAddress("my-metrics-eb-address")
                .metricStorageName("foobar")
                .metricRefreshPeriod(2)
                .memoryUsageLimitPercent(80)
                .redisReadyCheckIntervalMs(2000)
                .build()
                .asJsonObject();

        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        exceptionFactory = newWastefulExceptionFactory();
        memoryUsageProvider = new TestMemoryUsageProvider(Optional.of(50));
        redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(vertx, config))
                .withMeterRegistry(meterRegistry)
                .build();
        redisQues.disableMigrationTool();
        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - {} was successful.", redisQues.getClass().getSimpleName());
            jedis = new Jedis("localhost", 6379, 5000);
            keyspaceHelper = redisQues.getKeyspaceHelper();
            queueStatisticsCollector = redisQues.getqueueStatisticsCollector();
            async.complete();
        }));

    }

    @After
    public void after(TestContext context) {
        if (queueStatisticsCollector != null) {
            queueStatisticsCollector.stop();
        }
        vertx.close(context.asyncAssertSuccess());
    }

    /**
     * This test demonstrates a bug in getAllApproximateQueueSize():
     * When one consumer reports a queue with timestamp=0 (unknown), it unconditionally
     * overwrites data from another consumer that has a valid (newer) timestamp.
     * <p>
     * The ts==0 branch should only be used as a fallback when no other data exists,
     * not as an override that defeats the timestamp-based merge logic.
     * <p>
     * Note: The bug is order-dependent (ConcurrentHashMap iteration order).
     * We use multiple queues and consumers to increase probability of triggering it.
     */
    @Test
    public void testGetAllApproximateQueueSize_TimestampZeroShouldNotOverwriteNewerData(TestContext context) {
        Async async = context.async();

        // first consumer
        for (int i = 0; i < 10; i++) {
            String queueName = "queue-" + i;
            QueueProcessingState validState = new QueueProcessingState(QueueState.READY, 1000 + i);
            validState.setQueueItemSize(100 + i);
            redisQues.getQueueConsumerRunner().getMyQueues().put(queueName, validState);
        }
        // second fake consumer
        vertx.eventBus().consumer(
                keyspaceHelper.getQueueRunningStateKey(),
                msg -> {
                    JsonObject request = (JsonObject) msg.body();
                    String replyAddress = request.getString("reply");
                    long refreshesWithinMs = request.getLong(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS);
                    context.assertEquals(0L, refreshesWithinMs);
                    JsonObject response = new JsonObject();

                    for (int i = 0; i < 10; i++) {
                        String queueName = "queue-" + i;
                        QueueProcessingState validState = new QueueProcessingState(QueueState.READY, 0);
                        validState.setQueueItemSize(100 + i);
                        response.put(queueName, JsonObject.mapFrom(validState));
                    }
                    //delay few ms, let this record at 2 pos
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    vertx.eventBus().send(replyAddress, response);
                }
        );

        queueStatisticsCollector.getAllApproximateQueueSize().onComplete(event -> {
            int bugCount = 0;
            for (int i = 0; i < 10; i++) {
                String queueName = "queue-" + i;
                long staleValue = 1 + i;
                Long actual = event.result().get(queueName);

                if (actual != null && actual == staleValue) {
                    bugCount++;
                }
            }
            context.assertEquals(0, bugCount,
                    "Found " + bugCount + " queues where ts=0 data overwrote valid timestamp data. " +
                            "The ts==0 case should not overwrite data with valid timestamps.");
            async.complete();
        });

    }

    /**
     * This test verifies that timestamp-based merge works correctly when both
     * consumers have valid (non-zero) timestamps.
     */
    @Test
    public void testGetAllApproximateQueueSize_NewerTimestampWins(TestContext context) {
        Async async = context.async();

        // first consumer
        QueueProcessingState consumerAQueues = new QueueProcessingState(QueueState.READY, 1000);
        consumerAQueues.setQueueItemSize(10);
        redisQues.getQueueConsumerRunner().getMyQueues().put("foo", consumerAQueues);

        vertx.eventBus().consumer(
                keyspaceHelper.getQueueRunningStateKey(),
                msg -> {
                    JsonObject request = (JsonObject) msg.body();
                    String replyAddress = request.getString("reply");
                    long refreshesWithinMs = request.getLong(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS);
                    context.assertEquals(0L, refreshesWithinMs);
                    JsonObject response = new JsonObject();

                    QueueProcessingState consumerBQueues = new QueueProcessingState(QueueState.READY, 2000);
                    consumerBQueues.setQueueItemSize(20);
                    response.put("foo", JsonObject.mapFrom(consumerBQueues));
                    //delay few ms, let this record at 2 pos
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    vertx.eventBus().send(replyAddress, response);
                }
        );

        // Get merged result
        queueStatisticsCollector.getAllApproximateQueueSize().onComplete(event -> {
            // Expected: 20 (from consumer-B with the newer timestamp 2000)
            context.assertEquals(20L, event.result().get("foo"),
                    "Queue size should be 20 (from consumer-B with newer timestamp=2000)");
            async.complete();
        });
    }

    /**
     * This test verifies that timestamp=0 data is used when no other data exists.
     */
    @Test
    public void testGetAllApproximateQueueSize_TimestampZeroUsedAsFallback(TestContext context) {
        Async async = context.async();
        // Consumer A reports queue "foo" with timestamp=0, size=42
        QueueProcessingState consumerAQueues = new QueueProcessingState(QueueState.READY, 0);
        consumerAQueues.setQueueItemSize(42);
        redisQues.getQueueConsumerRunner().getMyQueues().put("foo", consumerAQueues);

        // Get merged result
        queueStatisticsCollector.getAllApproximateQueueSize().onComplete(new Handler<AsyncResult<Map<String, Long>>>() {
            @Override
            public void handle(AsyncResult<Map<String, Long>> event) {
                // Expected: 42 (ts=0 should be used when it's the only data available)
                context.assertEquals(42L, event.result().get("foo"),
                        "Queue size should be 42 when ts=0 is the only data available");
                async.complete();
            }
        });
    }

    /**
     * Regression test for a bug in {@code step1()} of {@code getQueueStatistics()} where the
     * queue-name/queue-key iterator was advanced twice per "cache miss" entry. This caused queue
     * sizes to be attached to the wrong queue name (or dropped entirely), i.e. the name/size
     * pairing became misaligned.
     * <p>
     * This test enqueues many queues with distinct, easily distinguishable sizes and verifies
     * that every requested queue is present exactly once in the result, with the size that
     * actually matches its own queue length in Redis.
     */
    @Test
    public void testGetQueueStatistics_NameAndSizeStayAligned(TestContext context) {
        Async async = context.async();

        List<String> queueNames = new ArrayList<>();
        Map<String, Integer> expectedSizes = new LinkedHashMap<>();
        int numQueues = 25;
        for (int i = 0; i < numQueues; i++) {
            String queueName = "queue-stats-order-" + i;
            int size = i + 1; // distinct size per queue so misalignment is detectable
            for (int j = 0; j < size; j++) {
                jedis.rpush(getQueuesRedisKeyPrefix() + queueName, "item" + j);
            }
            queueNames.add(queueName);
            expectedSizes.put(queueName, size);
        }

        queueStatisticsCollector.getQueueStatistics(queueNames, true).onComplete(context.asyncAssertSuccess(result -> {
            JsonArray queues = result.getJsonArray(QUEUES);
            context.assertEquals(numQueues, queues.size(),
                    "Expected exactly one statistics entry per requested queue");
            Set<String> seenNames = new HashSet<>();
            for (int i = 0; i < queues.size(); i++) {
                JsonObject entry = queues.getJsonObject(i);
                String name = entry.getString(MONITOR_QUEUE_NAME);
                long size = entry.getLong(MONITOR_QUEUE_SIZE);

                context.assertTrue(expectedSizes.containsKey(name),
                        "Unexpected queue name in result: " + name);
                context.assertEquals((long) expectedSizes.get(name), size,
                        "Queue '" + name + "' has wrong size - name/size pairing got misaligned");
                context.assertTrue(seenNames.add(name),
                        "Duplicate queue name in result: " + name);
            }
            context.assertEquals(expectedSizes.keySet(), seenNames,
                    "All requested queues must be present exactly once in the result");
            async.complete();
        }));
    }

    @Test
    public void testMergeQueueSizeFromAllQueueRunningStates_IgnoresMalformedEntries(TestContext context) {
        JsonObject validQueueState = new JsonObject()
                .put("queueItemSizeCounter", 11L)
                .put("lastRegisterRefreshedMillis", 1000L);
        JsonObject newerQueueState = new JsonObject()
                .put("queueItemSizeCounter", 22L)
                .put("lastRegisterRefreshedMillis", 2000L);
        JsonObject malformedQueueState = new JsonObject()
                .put("queueItemSizeCounter", "not-a-number")
                .put("lastRegisterRefreshedMillis", "bad-ts");

        JsonObject instanceA = new JsonObject()
                .put("queue-a", validQueueState)
                .put("queue-malformed", malformedQueueState)
                .put("queue-nonobject", "oops");
        JsonObject instanceB = new JsonObject()
                .put("queue-a", newerQueueState);

        JsonArray payload = new JsonArray()
                .add(instanceA)
                .add("invalid-instance-entry")
                .add(instanceB);

        Map<String, Long> merged = QueueStatisticsCollector.mergeQueueSizeFromAllQueueRunningStates(payload);
        context.assertEquals(1, merged.size());
        context.assertEquals(22L, merged.get("queue-a").longValue());
    }
}
