package org.swisspush.redisques.action;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Optional;

import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;
import static org.swisspush.redisques.util.RedisquesAPI.buildEnqueueOperation;

public class GetQueuesItemsCountActionExtTest extends AbstractTestCase {
    private RedisQues redisQues;
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
            async.complete();
        }));

    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void getQueueStatesFromAllVerticles(TestContext context) {
        Async async = context.async();
        flushAll();
        //Another fake Verticle
        vertx.eventBus().consumer(
                keyspaceHelper.getQueueRunningStateKey(),
                msg -> {
                    JsonObject request = (JsonObject) msg.body();
                    String replyAddress = request.getString("reply");
                    long refreshesWithinMs = request.getLong(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS);
                    context.assertEquals(30000L, refreshesWithinMs);
                    JsonObject response = new JsonObject();
                    response.put("queueName_1", new JsonObject());
                    response.put("queueName_2", new JsonObject());
                    response.put("queueName_3", new JsonObject());
                    //delay few ms, let this record at 2 pos
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    vertx.eventBus().send(replyAddress, response);
                }
        );
        eventBusSend(buildEnqueueOperation("queue1-test-batchsize", "message_1-1"), e1 -> {
            eventBusSend(buildEnqueueOperation("queue1-test-batchsize", "message_1-2"), e2 -> {
                eventBusSend(buildEnqueueOperation("queue1-test-batchsize", "message_1-3"), e3 -> {
                    eventBusSend(buildEnqueueOperation("queue2-test-batchsize", "message_2-1"), e4 -> {
                        eventBusSend(buildEnqueueOperation("queue2-test-batchsize", "message_2-2"), e5 -> {
                            eventBusSend(buildEnqueueOperation("queue2-test-batchsize", "message_2-3"), e6 -> {
                                eventBusSend(buildEnqueueOperation("queue3-test-batchsize", "message_3-1"), e7 -> {
                                    vertx.setTimer(1000, timerId -> {
                                        JsonObject requestBody = RedisquesAPI.buildGetQueuesItemsCountOperation(null);
                                        vertx.eventBus().request(keyspaceHelper.getAddress(), requestBody).onComplete(new Handler<AsyncResult<Message<Object>>>() {
                                            @Override
                                            public void handle(AsyncResult<Message<Object>> event) {
                                                context.assertFalse(event.failed());
                                                List<JsonObject> queues = ((JsonObject) event.result().body()).getJsonArray("queues").getList();
                                                context.assertEquals(3, queues.size());
                                                async.complete();
                                            }
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }
}

