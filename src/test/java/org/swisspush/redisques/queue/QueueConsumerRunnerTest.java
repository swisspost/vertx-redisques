package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
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
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.swisspush.redisques.util.RedisquesAPI.BATCH_QUEUE;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.buildAddQueueItemOperation;

public class QueueConsumerRunnerTest extends AbstractTestCase {
    private RedisQues redisQues;
    private TestMemoryUsageProvider memoryUsageProvider;

    private final String metricsIdentifier = "foo";

    @Rule
    public Timeout rule = Timeout.seconds(50);

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();
        QueueConfigurationProvider.reset();
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
                .queueConfigurations(List.of(new QueueConfiguration("queue.*")
                                .withRetryIntervals(2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52),
                        new QueueConfiguration("limited-queue-1.*")
                                .withMaxQueueEntries(1),
                        new QueueConfiguration("limited-queue-4.*")
                                .withMaxQueueEntries(4))
                )
                .queueConfigCleanupInterval(1_000L)
                .build()
                .asJsonObject();

        MeterRegistry meterRegistry = new SimpleMeterRegistry();

        memoryUsageProvider = new TestMemoryUsageProvider(Optional.of(50));
        redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(vertx, config))
                .withMeterRegistry(meterRegistry)
                .build();

        redisQues.disableMigrationTool();
        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            keyspaceHelper = redisQues.getKeyspaceHelper();
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

    @Test
    public void testMultipleItemBatchDispatchSuccessWithoutLimit(TestContext context) {
        Async async = context.async();
        final AtomicInteger index = new AtomicInteger(0);
        flushAll();
        vertx.eventBus().consumer(RedisquesConfiguration.PROP_PROCESSOR_ADDRESS, (Handler<Message<JsonObject>>) event -> {
            log.info("Received '{}'", event.body());
            if (index.get() == 0) {
                context.assertEquals("batch-queue-1.test", event.body().getString("queue"));
                context.assertTrue(event.body().getBoolean(BATCH_QUEUE));
                JsonArray batchItemsJsonObject = new JsonArray(event.body().getString("payload"));
                context.assertEquals(3, batchItemsJsonObject.size());
                // the order is important
                context.assertEquals("message_1-1", batchItemsJsonObject.getString(0));
                context.assertEquals("message_1-2", batchItemsJsonObject.getString(1));
                context.assertEquals("message_1-3", batchItemsJsonObject.getString(2));
            } else if (index.get() == 1) {
                context.assertEquals("batch-queue-4.test", event.body().getString("queue"));
                context.assertTrue(event.body().getBoolean(BATCH_QUEUE));
                JsonArray batchItemsJsonObject = new JsonArray(event.body().getString("payload"));
                context.assertEquals(5, batchItemsJsonObject.size());
                // the order is important
                context.assertEquals("message_4-1", batchItemsJsonObject.getString(0));
                context.assertEquals("message_4-2", batchItemsJsonObject.getString(1));
                context.assertEquals("message_4-3", batchItemsJsonObject.getString(2));
                context.assertEquals("message_4-4", batchItemsJsonObject.getString(3));
                context.assertEquals("message_4-5", batchItemsJsonObject.getString(4));
            } else {
                context.fail("unexpected index " + index);
            }
            event.reply(new JsonObject().put(STATUS, OK));
        });

        List<String[]> items = List.of(
                new String[]{"batch-queue-1.test", "message_1-1"},
                new String[]{"batch-queue-1.test", "message_1-2"},
                new String[]{"batch-queue-1.test", "message_1-3"},
                new String[]{"batch-queue-4.test", "message_4-1"},
                new String[]{"batch-queue-4.test", "message_4-2"},
                new String[]{"batch-queue-4.test", "message_4-3"},
                new String[]{"batch-queue-4.test", "message_4-4"},
                new String[]{"batch-queue-4.test", "message_4-5"},
                new String[]{"batch-queue-4.test", "message_4-6"},
                new String[]{"batch-queue-4.test", "message_4-7"},
                new String[]{"batch-queue-4.test", "message_4-8"}
        );

        addQueueItemsSequentially(items).onComplete(e11 -> {
            assertQueuesCount(context, 2);
            assertQueueItemsCount(context, "batch-queue-1.test", 3);
            assertQueueItemsCount(context, "batch-queue-4.test", 8);
            String queueName = "batch-queue-1.test";
            redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 0, 0).onComplete(new Handler<AsyncResult<Void>>() {
                @Override
                public void handle(AsyncResult<Void> event) {
                    index.incrementAndGet();
                    String queueName = "batch-queue-4.test";
                    redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 0, 0).onComplete(new Handler<AsyncResult<Void>>() {
                        @Override
                        public void handle(AsyncResult<Void> event) {
                            assertQueuesCount(context, 1);
                            assertQueueItemsCount(context, "batch-queue-1.test", 0);
                            assertQueueItemsCount(context, "batch-queue-4.test", 3);
                            async.complete();
                        }
                    });
                }
            });
        });
    }

    @Test
    public void testMultipleItemBatchDispatchFailed(TestContext context) {
        Async async = context.async();
        final AtomicInteger index = new AtomicInteger(0);
        flushAll();
        vertx.eventBus().consumer(RedisquesConfiguration.PROP_PROCESSOR_ADDRESS, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                log.info("Received '{}'", event.body());
                if (index.get() == 0) {
                    JsonArray batchItemsJsonObject = new JsonArray(event.body().getString("payload"));
                    context.assertTrue(Boolean.parseBoolean(event.body().getString(BATCH_QUEUE)));
                    context.assertEquals(3, batchItemsJsonObject.size());
                    // the order is important
                    context.assertEquals("message_1-1", batchItemsJsonObject.getString(0));
                    context.assertEquals("message_1-2", batchItemsJsonObject.getString(1));
                    context.assertEquals("message_1-3", batchItemsJsonObject.getString(2));
                } else if (index.get() == 1) {
                    JsonArray batchItemsJsonObject = new JsonArray(event.body().getString("payload"));
                    context.assertTrue(Boolean.parseBoolean(event.body().getString(BATCH_QUEUE)));
                    context.assertEquals(5, batchItemsJsonObject.size());
                    // the order is important
                    context.assertEquals("message_4-1", batchItemsJsonObject.getString(0));
                    context.assertEquals("message_4-2", batchItemsJsonObject.getString(1));
                    context.assertEquals("message_4-3", batchItemsJsonObject.getString(2));
                    context.assertEquals("message_4-4", batchItemsJsonObject.getString(3));
                    context.assertEquals("message_4-5", batchItemsJsonObject.getString(4));
                } else {
                    context.fail("unexpected index " + index);
                }
                event.reply(new JsonObject().put(STATUS, ERROR));
            }
        });
        List<String[]> items = List.of(
                new String[]{"batch-queue-1.test", "message_1-1"},
                new String[]{"batch-queue-1.test", "message_1-2"},
                new String[]{"batch-queue-1.test", "message_1-3"},
                new String[]{"batch-queue-4.test", "message_4-1"},
                new String[]{"batch-queue-4.test", "message_4-2"},
                new String[]{"batch-queue-4.test", "message_4-3"},
                new String[]{"batch-queue-4.test", "message_4-4"},
                new String[]{"batch-queue-4.test", "message_4-5"},
                new String[]{"batch-queue-4.test", "message_4-6"},
                new String[]{"batch-queue-4.test", "message_4-7"},
                new String[]{"batch-queue-4.test", "message_4-8"}
        );

        addQueueItemsSequentially(items).onComplete(e11 -> {
            assertQueuesCount(context, 2);
            assertQueueItemsCount(context, "batch-queue-1.test", 3);
            assertQueueItemsCount(context, "batch-queue-4.test", 8);
            String queueName = "batch-queue-1.test";
            redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 0, 0).onComplete(new Handler<AsyncResult<Void>>() {
                @Override
                public void handle(AsyncResult<Void> event) {
                    index.incrementAndGet();
                    String queueName = "batch-queue-4.test";
                    redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 0, 0).onComplete(new Handler<AsyncResult<Void>>() {
                        @Override
                        public void handle(AsyncResult<Void> event) {
                            assertQueuesCount(context, 2);
                            assertQueueItemsCount(context, "batch-queue-1.test", 3);
                            assertQueueItemsCount(context, "batch-queue-4.test", 8);
                            async.complete();
                        }
                    });
                }
            });
        });
    }

    @Test
    public void testMultipleItemBatchDispatchSuccessWithMinimum(TestContext context) {
        Async async = context.async();
        final AtomicInteger index = new AtomicInteger(0);
        final String queueName = "batch-queue-1.test";
        flushAll();
        vertx.eventBus().consumer(RedisquesConfiguration.PROP_PROCESSOR_ADDRESS, (Handler<Message<JsonObject>>) event -> {
            log.info("Received '{}'", event.body());
            if (index.get() == 0) {
                context.fail("Should no dispatch");
            } else if (index.get() == 1) {
                JsonArray batchItemsJsonObject = new JsonArray(event.body().getString("payload"));
                context.assertTrue(Boolean.parseBoolean(event.body().getString(BATCH_QUEUE)));
                context.assertEquals(4, batchItemsJsonObject.size());
                // the order is important
                context.assertEquals("message_1-1", batchItemsJsonObject.getString(0));
                context.assertEquals("message_1-2", batchItemsJsonObject.getString(1));
                context.assertEquals("message_1-3", batchItemsJsonObject.getString(2));
                context.assertEquals("message_1-4", batchItemsJsonObject.getString(3));
            } else {
                context.fail("unexpected index " + index);
            }
            event.reply(new JsonObject().put(STATUS, OK));
        });

        List<String[]> items = List.of(
                new String[]{queueName, "message_1-1"},
                new String[]{queueName, "message_1-2"},
                new String[]{queueName, "message_1-3"}
        );

        addQueueItemsSequentially(items).onComplete(e11 -> {
            assertQueuesCount(context, 1);
            assertQueueItemsCount(context, queueName, 3);
            redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 4, 0).onComplete(new Handler<AsyncResult<Void>>() {
                @Override
                public void handle(AsyncResult<Void> event) {
                    index.incrementAndGet();
                    eventBusSend(buildAddQueueItemOperation(queueName, "message_1-4"), e1 -> {
                        redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 4, 0).onComplete(new Handler<AsyncResult<Void>>() {
                            @Override
                            public void handle(AsyncResult<Void> event) {
                                assertQueuesCount(context, 0);
                                assertQueueItemsCount(context, queueName, 0);
                                async.complete();
                            }
                        });
                    });
                }
            });
        });
    }

    @Test
    public void testMultipleItemBatchDispatchSuccessWithMinimumAndTimeout(TestContext context) {
        Async async = context.async();
        final AtomicInteger index = new AtomicInteger(0);
        final String queueName = "batch-queue-1.test";
        flushAll();
        vertx.eventBus().consumer(RedisquesConfiguration.PROP_PROCESSOR_ADDRESS, (Handler<Message<JsonObject>>) event -> {
            log.info("Received '{}'", event.body());
            if (index.get() == 0) {
                context.fail("Should no dispatch");
            } else if (index.get() == 1) {
                context.fail("Should no dispatch");
            } else if (index.get() == 2) {
                context.fail("Should no dispatch");
            } else if (index.get() == 3) {
                JsonArray batchItemsJsonObject = new JsonArray(event.body().getString("payload"));
                context.assertTrue(Boolean.parseBoolean(event.body().getString(BATCH_QUEUE)));
                context.assertEquals(3, batchItemsJsonObject.size());
                // the order is important
                context.assertEquals("message_1-1", batchItemsJsonObject.getString(0));
                context.assertEquals("message_1-2", batchItemsJsonObject.getString(1));
                context.assertEquals("message_1-3", batchItemsJsonObject.getString(2));
            } else {
                context.fail("unexpected index " + index);
            }
            event.reply(new JsonObject().put(STATUS, OK));
        });

        List<String[]> items = List.of(
                new String[]{queueName, "message_1-1"},
                new String[]{queueName, "message_1-2"},
                new String[]{queueName, "message_1-3"}
        );
        // mock the consume function mark this queue is in consuming
        redisQues.getQueueConsumerRunner().getMyQueues().put(queueName, new QueueProcessingState(QueueState.CONSUMING, System.currentTimeMillis()));
        addQueueItemsSequentially(items).onComplete(e11 -> {
            assertQueuesCount(context, 1);
            assertQueueItemsCount(context, queueName, 3);
            redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 4, 3).onComplete(event -> {
                index.incrementAndGet();
                vertx.setTimer(1000, new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        index.incrementAndGet();
                        vertx.setTimer(1000, new Handler<Long>() {
                            @Override
                            public void handle(Long event) {
                                index.incrementAndGet();
                                vertx.setTimer(1100, new Handler<Long>() {
                                    @Override
                                    public void handle(Long event) {
                                        redisQues.getQueueConsumerRunner().processMultipleItems(queueName, 5, 4, 3).onComplete(event1 -> {
                                            assertQueuesCount(context, 0);
                                            assertQueueItemsCount(context, queueName, 0);
                                            async.complete();
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            });
        });
    }

    private Future<Void> addQueueItemsSequentially(List<String[]> items) {
        Promise<Void> promise = Promise.promise();
        addQueueItemsSequentially(items, 0, promise);
        return promise.future();
    }

    private void addQueueItemsSequentially(List<String[]> items, int index, Promise<Void> promise) {
        if (index >= items.size()) {
            promise.complete();
            return;
        }

        String queue = items.get(index)[0];
        String message = items.get(index)[1];

        eventBusSend(buildAddQueueItemOperation(queue, message), ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                return;
            }

            addQueueItemsSequentially(items, index + 1, promise);
        });
    }
}
