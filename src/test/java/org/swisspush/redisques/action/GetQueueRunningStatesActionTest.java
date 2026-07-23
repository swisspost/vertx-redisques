package org.swisspush.redisques.action;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
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
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.QueueProcessingState;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.Optional;

import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;

public class GetQueueRunningStatesActionTest extends AbstractTestCase {
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

    private void waitMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
                    context.assertEquals(0L, refreshesWithinMs);
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

        // queue last update at 10s before
        QueueProcessingState state1 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 10_000L);
        state1.setQueueItemSize(10);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_1", state1);
        // queue last update at 8s before
        QueueProcessingState state2 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 8_000L);
        state2.setQueueItemSize(20);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_2", state2);
        // queue last update at 4s before
        QueueProcessingState state3 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 4_000L);
        state3.setQueueItemSize(30);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_3", state3);

        QueueProcessingState state4 = new QueueProcessingState(QueueState.READY, 0);
        state4.setQueueItemSize(40);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_4", state4);

        JsonObject requestBody = RedisquesAPI.buildGetQueueRunningStates(0, 0, 0);
        GetQueueRunningStatesAction action = new GetQueueRunningStatesAction(vertx, keyspaceHelper, log);

        Message<JsonObject> message = new Message<>() {
            @Override
            public String address() {
                return "";
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public JsonObject body() {
                return requestBody;
            }

            @Override
            public String replyAddress() {
                return "";
            }

            @Override
            public boolean isSend() {
                return false;
            }

            @Override
            public void reply(Object message, DeliveryOptions options) {
                JsonObject jsonObject = (JsonObject) message;
                context.assertEquals(2, jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).size());
                context.assertEquals(4, ((JsonObject) jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).getList().get(0)).getMap().size());
                context.assertEquals(3, ((JsonObject) jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).getList().get(1)).getMap().size());
                async.complete();
            }

            @Override
            public <R> Future<Message<R>> replyAndRequest(Object message, DeliveryOptions options) {
                return null;
            }
        };
        action.execute(message);
    }

    @Test
    public void getQueueStatesFromAllVerticles_WithTimeout(TestContext context) {
        Async async = context.async();
        flushAll();
        //Another fake Verticle
        vertx.eventBus().consumer(
                keyspaceHelper.getQueueRunningStateKey(),
                msg -> {
                    JsonObject request = (JsonObject) msg.body();
                    String replyAddress = request.getString("reply");
                    long refreshesWithinMs = request.getLong(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS);
                    context.assertEquals(0L, refreshesWithinMs);
                    JsonObject response = new JsonObject();
                    response.put("queueName_1", new JsonObject());
                    response.put("queueName_2", new JsonObject());
                    response.put("queueName_3", new JsonObject());
                    vertx.setTimer(5_000L, event -> vertx.eventBus().send(replyAddress, response));
                }
        );

        // queue last update at 10s before
        QueueProcessingState state1 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 10_000L);
        state1.setQueueItemSize(10);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_1", state1);
        // queue last update at 8s before
        QueueProcessingState state2 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 8_000L);
        state2.setQueueItemSize(20);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_2", state2);
        // queue last update at 4s before
        QueueProcessingState state3 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 4_000L);
        state3.setQueueItemSize(30);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_3", state3);

        QueueProcessingState state4 = new QueueProcessingState(QueueState.READY, 0);
        state4.setQueueItemSize(40);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_4", state4);

        JsonObject requestBody = RedisquesAPI.buildGetQueueRunningStates(0, 0, 1_000);
        GetQueueRunningStatesAction action = new GetQueueRunningStatesAction(vertx, keyspaceHelper, log);

        Message<JsonObject> message = new Message<>() {
            @Override
            public String address() {
                return "";
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public JsonObject body() {
                return requestBody;
            }

            @Override
            public String replyAddress() {
                return "";
            }

            @Override
            public boolean isSend() {
                return false;
            }

            @Override
            public void reply(Object message, DeliveryOptions options) {
                JsonObject jsonObject = (JsonObject) message;
                context.assertEquals(1, jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).size());
                context.assertEquals(4, ((JsonObject) jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).getList().get(0)).getMap().size());
                async.complete();
            }

            @Override
            public <R> Future<Message<R>> replyAndRequest(Object message, DeliveryOptions options) {
                return null;
            }
        };
        action.execute(message);
    }

    @Test
    public void getQueueStatesFromAllVerticles_ExpectedReplies(TestContext context) {
        Async async = context.async();
        flushAll();
        long startTime = System.currentTimeMillis();
        //Another fake Verticle
        vertx.eventBus().consumer(
                keyspaceHelper.getQueueRunningStateKey(),
                msg -> {
                    JsonObject request = (JsonObject) msg.body();
                    String replyAddress = request.getString("reply");
                    long refreshesWithinMs = request.getLong(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS);
                    context.assertEquals(0L, refreshesWithinMs);
                    JsonObject response = new JsonObject();
                    response.put("queueName_1", new JsonObject());
                    response.put("queueName_2", new JsonObject());
                    response.put("queueName_3", new JsonObject());
                    vertx.setTimer(1_000L, event -> vertx.eventBus().send(replyAddress, response));
                }
        );

        // queue last update at 10s before
        QueueProcessingState state1 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 10_000L);
        state1.setQueueItemSize(10);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_1", state1);
        // queue last update at 8s before
        QueueProcessingState state2 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 8_000L);
        state2.setQueueItemSize(20);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_2", state2);
        // queue last update at 4s before
        QueueProcessingState state3 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 4_000L);
        state3.setQueueItemSize(30);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_3", state3);

        QueueProcessingState state4 = new QueueProcessingState(QueueState.READY, 0);
        state4.setQueueItemSize(40);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_4", state4);

        JsonObject requestBody = RedisquesAPI.buildGetQueueRunningStates(0, 2, 10_000);
        GetQueueRunningStatesAction action = new GetQueueRunningStatesAction(vertx, keyspaceHelper, log);

        Message<JsonObject> message = new Message<>() {
            @Override
            public String address() {
                return "";
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public JsonObject body() {
                return requestBody;
            }

            @Override
            public String replyAddress() {
                return "";
            }

            @Override
            public boolean isSend() {
                return false;
            }

            @Override
            public void reply(Object message, DeliveryOptions options) {
                JsonObject jsonObject = (JsonObject) message;
                context.assertEquals(2, jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).size());
                context.assertTrue(System.currentTimeMillis() - startTime < 1200);
                async.complete();
            }

            @Override
            public <R> Future<Message<R>> replyAndRequest(Object message, DeliveryOptions options) {
                return null;
            }
        };
        action.execute(message);
    }


    @Test
    public void getQueueStatesFromAllVerticles_OnlyWithinTime(TestContext context) {
        Async async = context.async();
        flushAll();
        long startTime = System.currentTimeMillis();
        //Another fake Verticle
        vertx.eventBus().consumer(
                keyspaceHelper.getQueueRunningStateKey(),
                msg -> {
                    JsonObject request = (JsonObject) msg.body();
                    String replyAddress = request.getString("reply");
                    long refreshesWithinMs = request.getLong(RedisquesAPI.GET_QUEUE_RUNNING_STATES_LAST_UPDATE_WITHIN_MS);
                    context.assertEquals(9000L, refreshesWithinMs);
                    JsonObject response = new JsonObject();
                    response.put("queueName_1", new JsonObject());
                    response.put("queueName_2", new JsonObject());
                    response.put("queueName_3", new JsonObject());
                    vertx.setTimer(2_000L, event -> vertx.eventBus().send(replyAddress, response));
                }
        );

        // queue last update at 10s before
        QueueProcessingState state1 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 10_000L);
        state1.setQueueItemSize(10);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_1", state1);
        // queue last update at 8s before
        QueueProcessingState state2 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 8_000L);
        state2.setQueueItemSize(20);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_2", state2);
        // queue last update at 4s before
        QueueProcessingState state3 = new QueueProcessingState(QueueState.READY, System.currentTimeMillis() - 4_000L);
        state3.setQueueItemSize(30);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_3", state3);

        QueueProcessingState state4 = new QueueProcessingState(QueueState.READY, 0);
        state4.setQueueItemSize(40);
        redisQues.getQueueConsumerRunner().getMyQueues().put("queue_4", state4);

        JsonObject requestBody = RedisquesAPI.buildGetQueueRunningStates(9_000L, 0, 1_000);
        GetQueueRunningStatesAction action = new GetQueueRunningStatesAction(vertx, keyspaceHelper, log);

        Message<JsonObject> message = new Message<>() {
            @Override
            public String address() {
                return "";
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public JsonObject body() {
                return requestBody;
            }

            @Override
            public String replyAddress() {
                return "";
            }

            @Override
            public boolean isSend() {
                return false;
            }

            @Override
            public void reply(Object message, DeliveryOptions options) {
                JsonObject jsonObject = (JsonObject) message;
                context.assertEquals(1, jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).size());
                context.assertEquals(2, ((JsonObject) jsonObject.getJsonArray(RedisquesAPI.PAYLOAD).getList().get(0)).getMap().size());
                context.assertTrue(System.currentTimeMillis() - startTime < 1200);
                async.complete();
            }

            @Override
            public <R> Future<Message<R>> replyAndRequest(Object message, DeliveryOptions options) {
                return null;
            }
        };
        action.execute(message);
    }
}
