package org.swisspush.redisques.migration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.action.AbstractQueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.migration.tasks.Task;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class MigrateToolTest extends AbstractTestCase {
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
            jedis = new Jedis("localhost", 6379, 5000);
            async.complete();
        }));
    }

    @Test
    public void testMigrateToolLeadLock(TestContext context) {
        Async async = context.async();
        AtomicInteger counter = new AtomicInteger(2);
        flushAllCluster();
        Task task1 = new Task() {
            private boolean doneOnce = false;
            @Override
            public String getTaskKey() {
                return "Test-Task-1";
            }

            @Override
            public Future<Boolean> run() {
                Promise<Boolean> promise = Promise.promise();
                    vertx.setTimer(2000, event -> {
                        context.assertFalse(doneOnce);
                        doneOnce = true;
                        promise.complete(true);
                    });
                return promise.future();
            }
        };

        Task task2 = new Task() {
            private boolean doneOnce = false;
            @Override
            public String getTaskKey() {
                return "Test-Task-2";
            }

            @Override
            public Future<Boolean> run() {
                Promise<Boolean> promise = Promise.promise();
                vertx.setTimer(3000, event -> {
                    context.assertFalse(doneOnce);
                    doneOnce = true;
                    promise.complete(true);
                });
                return promise.future();
            }
        };


        MigrateTool migrateToolA = new MigrateTool(vertx, redisQues.getConfigurationProvider(), redisQues.getRedisService(), redisQues.getKeyspaceHelper(), "MIGRATOR-1");
        MigrateTool migrateToolB = new MigrateTool(vertx, redisQues.getConfigurationProvider(), redisQues.getRedisService(), redisQues.getKeyspaceHelper(), "MIGRATOR-2");
        migrateToolA.addTask(task1).addTask(task2);
        migrateToolB.addTask(task1).addTask(task2);

        migrateToolB.start().onComplete(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                if(counter.decrementAndGet() == 0) {
                    async.complete();
                }
            }
        });
        migrateToolA.start().onComplete(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                if(counter.decrementAndGet() == 0) {
                    async.complete();
                }
            }
        });
    }

}
