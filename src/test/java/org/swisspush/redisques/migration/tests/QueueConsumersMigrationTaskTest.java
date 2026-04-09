package org.swisspush.redisques.migration.tests;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.redis.client.RedisClientType;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.action.AbstractQueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.migration.tasks.QueueConsumersMigrationTask;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This test need cluster enabled redis to run
 */
@Ignore
public class QueueConsumersMigrationTaskTest extends AbstractTestCase {
    private RedisQues redisQues;
    private TestMemoryUsageProvider memoryUsageProvider;
    private final String metricsIdentifier = "foo";
    protected AbstractQueueAction action;
    protected RedisQuesExceptionFactory exceptionFactory;

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();
        Async async = context.async();
        QueueConfigurationProvider.reset();
        RedisquesConfiguration rqConfig =  RedisquesConfiguration.with()
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
                .redisClientType(RedisClientType.CLUSTER)
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
                .build();

        MeterRegistry meterRegistry = new SimpleMeterRegistry();

        memoryUsageProvider = new TestMemoryUsageProvider(Optional.of(50));
        redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(vertx, rqConfig.asJsonObject()))
                .withMeterRegistry(meterRegistry)
                .build();
        redisQues.disableMigrationTool();
        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(rqConfig.asJsonObject()), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - {} was successful.", redisQues.getClass().getSimpleName());
            HostAndPort node = new HostAndPort("localhost", 6379);
            jedisCluster = new JedisCluster(node, 5000);
            async.complete();
        }));
    }

    @Test
    public void testMigrationProcess(TestContext context) {
        Async async = context.async();
        flushAllCluster();
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key1" ,"value1");
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key2" ,"value1");
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key3" ,"value1");
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key4" ,"value1");
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key5" ,"value1");
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key6" ,"value1");
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key7" ,"value1");
        new QueueConsumersMigrationTask(vertx, redisQues.getRedisService(), redisQues.getKeyspaceHelper()).run().onComplete(event -> {
            if (event.succeeded()) {
                Set<String> result = jedisCluster.keys(redisQues.getKeyspaceHelper().getClusterSafeConsumersPrefix() +"*" );
                context.assertEquals(7, result.size());
                async.complete();
            }else {
                context.fail(event.cause());
            }
        });
    }

    @Test
    public void testMigrationProcessWithEmptyData(TestContext context) {
        Async async = context.async();
        flushAllCluster();
        new QueueConsumersMigrationTask(vertx, redisQues.getRedisService(), redisQues.getKeyspaceHelper()).run().onComplete(event -> {
            if (event.succeeded()) {
                Set<String> result = jedisCluster.keys(redisQues.getKeyspaceHelper().getClusterSafeConsumersPrefix() +"*" );
                context.assertEquals(0, result.size());
                async.complete();
            }else {
                context.fail(event.cause());
            }
        });
    }

    @Test
    public void testMigrationNoDoubleProcess(TestContext context) {
        Async async = context.async();
        flushAllCluster();
        jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key1" ,"value1");
        new QueueConsumersMigrationTask(vertx, redisQues.getRedisService(), redisQues.getKeyspaceHelper()).run().onComplete(event -> {
            if (event.succeeded()) {
                Set<String> result = jedisCluster.keys(redisQues.getKeyspaceHelper().getClusterSafeConsumersPrefix() +"*" );
                context.assertEquals(1, result.size());
                context.assertEquals("value1", jedisCluster.get(result.stream().findFirst().get()));
                jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key1" ,"value2");
                jedisCluster.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "Key2" ,"value1");
                if (event.succeeded()) {
                    result = jedisCluster.keys(redisQues.getKeyspaceHelper().getClusterSafeConsumersPrefix() +"*" );
                    context.assertEquals(1, result.size());
                    async.complete();
                }else {
                    context.fail(event.cause());
                }
            }else {
                context.fail(event.cause());
            }
        });
    }

}
