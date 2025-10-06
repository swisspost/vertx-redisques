package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Optional;

public class QueueConsumerRunnerTest extends AbstractTestCase {
    private RedisQues redisQues;
    private TestMemoryUsageProvider memoryUsageProvider;

    private final String metricsIdentifier = "foo";

    @Rule
    public Timeout rule = Timeout.seconds(50);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();

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
        }));
        
    }
    @Test
    public void getQueueRescheduleRefreshPeriodWhileFailureCountIncreasedUntilExceedMaxRetryInterval(TestContext context) {
        final String queue = "queue1";

        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong when failure count is 0.");

        // send message fail
        context.assertEquals(2, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 1.");

        // send message fail
        context.assertEquals(7, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 2.");

        // send message fail
        context.assertEquals(12, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 3.");

        // send message fail
        context.assertEquals(17, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 4.");

        // send message fail
        context.assertEquals(22, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 5.");

        // send message fail
        context.assertEquals(27, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 6.");

        // send message fail
        context.assertEquals(32, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 7.");

        // send message fail
        context.assertEquals(37, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 8.");

        // send message fail
        context.assertEquals(42, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 9.");

        // send message fail
        context.assertEquals(47, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 10.");

        // send message fail
        context.assertEquals(52, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 11.");

        // already reach the max retry interval

        // send message fail
        context.assertEquals(52, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 12.");
    }

    @Test
    public void getQueueRescheduleRefreshPeriodAfterProcessMessageFail(TestContext context) {
        final String queue = "queue1";

        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");

        // send message fail
        context.assertEquals(2, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");

        // send message fail
        context.assertEquals(7, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");
    }

    @Test
    public void getQueueRescheduleRefreshPeriodAfterProcessMessageSuccess(TestContext context) {
        final String queue = "queue1";

        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");

        // send message fail
        context.assertEquals(2, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");

        // send message success
        context.assertEquals(0, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");
    }

    @Test
    public void getRescheduleRefreshPeriodOfUnknownQueue(TestContext context) {
        final String queue = "unknownqueue";

        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");

        // send message fail
        context.assertEquals(2, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");

        // send message fail
        // still use the default refresh period
        context.assertEquals(2, redisQues.getQueueConsumerRunner().updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");
    }

}
