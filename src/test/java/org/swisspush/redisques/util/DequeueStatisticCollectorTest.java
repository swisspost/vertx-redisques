package org.swisspush.redisques.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Optional;

@RunWith(VertxUnitRunner.class)
public class DequeueStatisticCollectorTest extends AbstractTestCase {

    private Vertx vertx;
    private RedisService redisService;
    private KeyspaceHelper keyspaceHelper;
    private DequeueStatisticCollector dequeueStatisticCollectorEnabled;
    private DequeueStatisticCollector dequeueStatisticCollectorDisabled;
    private RedisQues redisQues;
    private TestMemoryUsageProvider memoryUsageProvider;

    @Rule
    public Timeout rule = Timeout.seconds(50);

    @Before
    public void deployRedisques(TestContext context) {
        Async async = context.async();
        vertx = Vertx.vertx();

        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .micrometerMetricsEnabled(true)
                .micrometerPerQueueMetricsEnabled(true)
                .refreshPeriod(2)
                .publishMetricsAddress("my-metrics-eb-address")
                .metricStorageName("foobar")
                .metricRefreshPeriod(2)
                .memoryUsageLimitPercent(80)
                .redisReadyCheckIntervalMs(2000)
                .queueConfigurations(List.of(new QueueConfiguration()
                                .withPattern("queue.*")
                                .withRetryIntervals(2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52),
                        new QueueConfiguration()
                                .withPattern("limited-queue-1.*")
                                .withMaxQueueEntries(1),
                        new QueueConfiguration()
                                .withPattern("limited-queue-4.*")
                                .withMaxQueueEntries(4))
                )
                .build()
                .asJsonObject();

        MeterRegistry meterRegistry = new SimpleMeterRegistry();

        memoryUsageProvider = new TestMemoryUsageProvider(Optional.of(50));
        redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(vertx, config))
                .withMeterRegistry(meterRegistry)
                .build();

        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - {} was successful.", redisQues.getClass().getSimpleName());
            jedis = new Jedis("localhost", 6379, 5000);
            redisService = redisQues.getRedisService();
            keyspaceHelper = redisQues.getKeyspaceHelper();
            // Initialize DequeueStatisticCollector with enabled/disabled stats collection
            dequeueStatisticCollectorEnabled = new DequeueStatisticCollector(true, redisService, keyspaceHelper);
            dequeueStatisticCollectorDisabled = new DequeueStatisticCollector(false, redisService, keyspaceHelper);
            async.complete();
        }));
    }

    @Test
    public void testGetAllAndRemoveWhenDequeueStatisticsEnabled(TestContext context) {
        String queueName = "queue1";
        // Test when dequeue statistics are enabled
        Async async = context.async();
        dequeueStatisticCollectorEnabled.setDequeueStatistic(queueName, new DequeueStatistic(queueName)).onComplete(v ->
                dequeueStatisticCollectorEnabled.getAllDequeueStatistics().onComplete(result -> {
                    context.assertTrue(result.succeeded());
                    context.assertEquals(1, result.result().size());
                    DequeueStatistic dequeueStatisticRemove = new DequeueStatistic(queueName);
                    dequeueStatisticRemove.setMarkedForRemoval();
                    dequeueStatisticCollectorEnabled.setDequeueStatistic(queueName, dequeueStatisticRemove).onComplete(s ->
                            dequeueStatisticCollectorEnabled.getAllDequeueStatistics().onComplete(result1 -> {
                                context.assertTrue(result1.succeeded());
                                context.assertEquals(0, result1.result().size());
                                async.complete();
                            }));
                }));
    }

    @Test
    public void testGetAllDequeueStatisticsDisabled(TestContext context) {
        // Test when dequeue statistics are disabled
        String queueName = "queue2";
        Async async = context.async();
        dequeueStatisticCollectorDisabled.setDequeueStatistic(queueName, new DequeueStatistic(queueName)).onComplete(v ->
                dequeueStatisticCollectorDisabled.getAllDequeueStatistics().onComplete(result -> {
                    context.assertTrue(result.succeeded());
                    context.assertEquals(0, result.result().size());
                    DequeueStatistic dequeueStatisticRemove = new DequeueStatistic(queueName);
                    dequeueStatisticRemove.setMarkedForRemoval();
                    dequeueStatisticCollectorDisabled.setDequeueStatistic(queueName, dequeueStatisticRemove).onComplete(s ->
                            dequeueStatisticCollectorDisabled.getAllDequeueStatistics().onComplete(result1 -> {
                                context.assertTrue(result1.succeeded());
                                context.assertEquals(0, result1.result().size());
                                async.complete();
                            }));
                }));
    }

}
