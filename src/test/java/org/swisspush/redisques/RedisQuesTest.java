package org.swisspush.redisques;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.util.*;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class RedisQueueBrowserTest.
 *
 * @author baldim, <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class RedisQuesTest extends AbstractTestCase {

    private RedisQues redisQues;
    private TestMemoryUsageProvider memoryUsageProvider;
    private Counter enqueueCounterSuccess;
    private Counter enqueueCounterFail;
    private Counter dequeueCounter;

    private final String metricsIdentifier = "foo";

    @Rule
    public Timeout rule = Timeout.seconds(50);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();

        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .micrometerMetricsEnabled(true)
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
        enqueueCounterSuccess = meterRegistry.counter(MetricMeter.ENQUEUE_SUCCESS.getId(), MetricTags.IDENTIFIER.getId(), metricsIdentifier);
        enqueueCounterFail = meterRegistry.counter(MetricMeter.ENQUEUE_FAIL.getId(), MetricTags.IDENTIFIER.getId(), metricsIdentifier);
        dequeueCounter =  meterRegistry.counter(MetricMeter.DEQUEUE.getId(), MetricTags.IDENTIFIER.getId(), metricsIdentifier);

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
    public void testPublishRedisMetrics(TestContext context) {
        Async async = context.async();
        AtomicBoolean messageReceived = new AtomicBoolean(false);

        vertx.eventBus().localConsumer("my-metrics-eb-address", (Handler<Message<JsonObject>>) message -> {
            context.assertTrue(message.body().getString("name").startsWith("redis.foobar."));
            if(!messageReceived.get()) {
                messageReceived.set(true);
                async.complete();
            }
        });
    }

    @Test
    public void testUnsupportedOperation(TestContext context) {
        Async async = context.async();
        JsonObject op = new JsonObject();
        op.put(OPERATION, "some_unkown_operation");
        eventBusSend(op, message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("QUEUE_ERROR: Unsupported operation received: some_unkown_operation", message.result().body().getString(MESSAGE));
            async.complete();
        });
    }

    @Test
    public void getConfiguration(TestContext context) {
        Async async = context.async();
        eventBusSend(buildGetConfigurationOperation(), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonObject configuration = message.result().body().getJsonObject(VALUE);
            context.assertNotNull(configuration);

            context.assertEquals(configuration.getString("address"), "redisques");
            context.assertEquals(configuration.getString("processor-address"), "processor-address");

            context.assertEquals(configuration.getString("redisHost"), "localhost");
            context.assertEquals(configuration.getInteger("redisPort"), 6379);

            context.assertEquals(configuration.getJsonArray("redisHosts").getList(), Collections.singletonList("localhost"));
            context.assertEquals(configuration.getJsonArray("redisPorts").getList(), Collections.singletonList(6379));
            context.assertFalse(configuration.getBoolean("redisEnableTls"));

            context.assertEquals(configuration.getString("redis-prefix"), "redisques:");

            context.assertEquals(configuration.getInteger("maxPoolSize"), 200);
            context.assertEquals(configuration.getInteger("maxPoolWaitingSize"), Integer.MAX_VALUE);
            context.assertEquals(configuration.getInteger("maxPipelineWaitingSize"), 2048);

            context.assertEquals(configuration.getInteger("checkInterval"), 60);
            context.assertEquals(configuration.getInteger("queueSpeedIntervalSec"), 60);
            context.assertEquals(configuration.getInteger("memoryUsageLimitPercent"), 80);
            context.assertEquals(configuration.getInteger("refresh-period"), 2);
            context.assertEquals(configuration.getInteger("processorTimeout"), 240000);
            context.assertEquals(configuration.getLong("processorDelayMax"), 0L);
            context.assertTrue(configuration.getBoolean("enableQueueNameDecoding"));

            context.assertFalse(configuration.getBoolean("httpRequestHandlerEnabled"));
            context.assertEquals(configuration.getInteger("httpRequestHandlerPort"), 7070);
            context.assertEquals(configuration.getString("httpRequestHandlerPrefix"), "/queuing");
            context.assertEquals(configuration.getString("httpRequestHandlerUserHeader"), "x-rp-usr");

            context.assertEquals(3, configuration.getJsonArray("queueConfigurations").size());
            context.assertEquals("queue.*", configuration.getJsonArray("queueConfigurations").getJsonObject(0).getString("pattern"));

            async.complete();
        });
    }

    @Test
    public void setConfigurationImplementedValuesOnly(TestContext context) {
        Async async = context.async();
        eventBusSend(buildOperation(QueueOperation.setConfiguration,
                new JsonObject().put(PROCESSOR_DELAY_MAX, 99).put("redisHost", "anotherHost").put("redisPort", 1234)), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("Not supported configuration values received: [redisHost, redisPort]", message.result().body().getString(MESSAGE));
            async.complete();
        });
    }

    @Test
    public void setConfigurationWrongDataType(TestContext context) {
        Async async = context.async();
        eventBusSend(buildOperation(QueueOperation.setConfiguration,
                new JsonObject().put(PROCESSOR_DELAY_MAX, "a_string_value")), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is not a number", message.result().body().getString(MESSAGE));
            async.complete();
        });
    }

    @Test
    public void setConfigurationProcessorDelayMax(TestContext context) {
        Async async = context.async();
        eventBusSend(buildGetConfigurationOperation(), getConfig -> {
            context.assertEquals(OK, getConfig.result().body().getString(STATUS));
            JsonObject configuration = getConfig.result().body().getJsonObject(VALUE);
            context.assertNotNull(configuration);
            context.assertEquals(configuration.getLong(PROCESSOR_DELAY_MAX), 0L);

            eventBusSend(buildSetConfigurationOperation(new JsonObject().put(PROCESSOR_DELAY_MAX, 1234)), setConfig -> {
                context.assertEquals(OK, setConfig.result().body().getString(STATUS));
                eventBusSend(buildGetConfigurationOperation(), getConfigAgain -> {
                    context.assertEquals(OK, getConfigAgain.result().body().getString(STATUS));
                    JsonObject updatedConfig = getConfigAgain.result().body().getJsonObject(VALUE);
                    context.assertNotNull(updatedConfig);
                    context.assertEquals(updatedConfig.getLong(PROCESSOR_DELAY_MAX), 1234L);
                    async.complete();
                });
            });
        });
    }

    @Test
    public void enqueue(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals("helloEnqueue", jedis.lindex(getQueuesRedisKeyPrefix() + "queueEnqueue", 0));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);

            assertEnqueueCounts(context, 1.0, 0.0);

            async.complete();
        });
    }

    @Test
    public void enqueueWithReachedMemoryUsageLimit(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        memoryUsageProvider.setCurrentMemoryUsage(Optional.of(90));
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("memory usage limit reached", message.result().body().getString(MESSAGE));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

            assertEnqueueCounts(context, 0.0, 1.0);

            //reduce current memory usage below the limit
            memoryUsageProvider.setCurrentMemoryUsage(Optional.of(50));

            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                context.assertEquals("helloEnqueue", jedis.lindex(getQueuesRedisKeyPrefix() + "queueEnqueue", 0));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
                context.assertEquals(1.0, enqueueCounterSuccess.count());
                async.complete();
            });
        });
    }

    @Test
    public void lockedEnqueue(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        eventBusSend(buildLockedEnqueueOperation("queueEnqueue", "helloEnqueue", "someuser"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals("helloEnqueue", jedis.lindex(getQueuesRedisKeyPrefix() + "queueEnqueue", 0));
            assertLockExists(context, "queueEnqueue");
            assertLockContent(context, "queueEnqueue", "someuser");
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
            assertKeyCount(context, getLocksRedisKey(), 1);
            assertEnqueueCounts(context, 1.0, 0.0);
            async.complete();
        });
    }

    @Test
    public void lockedEnqueueWithReachedMemoryUsageLimit(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        memoryUsageProvider.setCurrentMemoryUsage(Optional.of(90));
        eventBusSend(buildLockedEnqueueOperation("queueEnqueue", "helloEnqueue", "someuser"), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("memory usage limit reached", message.result().body().getString(MESSAGE));
            assertLockDoesNotExist(context, "queueEnqueue");
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
            assertKeyCount(context, getLocksRedisKey(), 0);
            assertEnqueueCounts(context, 0.0, 1.0);

            //reduce current memory usage below the limit
            memoryUsageProvider.setCurrentMemoryUsage(Optional.of(50));

            eventBusSend(buildLockedEnqueueOperation("queueEnqueue", "helloEnqueue", "someuser"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                context.assertEquals("helloEnqueue", jedis.lindex(getQueuesRedisKeyPrefix() + "queueEnqueue", 0));
                assertLockExists(context, "queueEnqueue");
                assertLockContent(context, "queueEnqueue", "someuser");
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
                assertKeyCount(context, getLocksRedisKey(), 1);
                assertEnqueueCounts(context, 1.0, 1.0);
                async.complete();
            });
        });
    }

    @Test
    public void lockedEnqueueMissingRequestedBy(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getLocksRedisKey(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertLockDoesNotExist(context, "queue1");

        JsonObject operation = buildOperation(QueueOperation.lockedEnqueue, new JsonObject().put(QUEUENAME, "queue1"));
        operation.put(MESSAGE, "helloEnqueue");

        eventBusSend(operation, message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("Property '" + REQUESTED_BY + "' missing", message.result().body().getString(MESSAGE));
            assertKeyCount(context, getLocksRedisKey(), 0);
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
            assertLockDoesNotExist(context, "queue1");
            assertEnqueueCounts(context, 0.0, 1.0);
            async.complete();
        });
    }

    @Test
    public void getQueueItems(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildGetQueueItemsOperation("queue1", null), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(0, message.result().body().getJsonArray(VALUE).size());
            eventBusSend(buildEnqueueOperation("queue1", "a_queue_item"), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
                eventBusSend(buildGetQueueItemsOperation("queue1", null), event -> {
                    context.assertEquals(OK, event.result().body().getString(STATUS));
                    context.assertEquals(1, event.result().body().getJsonArray(VALUE).size());
                    context.assertEquals("a_queue_item", event.result().body().getJsonArray(VALUE).getString(0));
                    context.assertEquals(1, event.result().body().getJsonArray(INFO).getInteger(0));
                    context.assertEquals(1, event.result().body().getJsonArray(INFO).getInteger(1));
                    async.complete();
                });
            });
        });
    }

    @Test
    public void getQueues(TestContext context) {
        Async asyncEnqueue = context.async(100);
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        for (int i = 0; i < 100; i++) {
            eventBusSend(buildEnqueueOperation("queue" + i, "testItem"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                asyncEnqueue.countDown();
            });

        }
        asyncEnqueue.awaitSuccess();

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 100);
        Async async = context.async();
        eventBusSend(buildGetQueuesOperation(), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonArray queuesArray = message.result().body().getJsonObject(VALUE).getJsonArray("queues");
            context.assertEquals(100, queuesArray.size());
            for (int i = 0; i < 100; i++) {
                context.assertTrue(queuesArray.contains("queue" + i), "item queue" + i + " expected to be in result");
            }
            async.complete();
        });
    }

    @Test
    public void getQueuesFiltered(TestContext context) {
        Async asyncEnqueue = context.async(20);
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        for (int i = 0; i < 20; i++) {
            eventBusSend(buildEnqueueOperation("queue" + i, "testItem"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                asyncEnqueue.countDown();
            });
        }
        asyncEnqueue.awaitSuccess();

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 20);
        Async async = context.async();
        eventBusSend(buildGetQueuesOperation("3"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonArray queuesArray = message.result().body().getJsonObject(VALUE).getJsonArray("queues");
            context.assertEquals(2, queuesArray.size());
            context.assertTrue(queuesArray.contains("queue3"), "item queue1 expected to be in result");
            context.assertTrue(queuesArray.contains("queue13"), "item queue11 expected to be in result");

            eventBusSend(buildGetQueuesOperation("abc"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                JsonArray queuesArray2 = message2.result().body().getJsonObject(VALUE).getJsonArray("queues");
                context.assertTrue(queuesArray2.isEmpty());
                async.complete();
            });
        });
    }

    @Test
    public void getQueuesFilteredInvalidPattern(TestContext context) {
        Async asyncEnqueue = context.async(20);
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        for (int i = 0; i < 20; i++) {
            eventBusSend(buildEnqueueOperation("queue" + i, "testItem"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                asyncEnqueue.countDown();
            });

        }
        asyncEnqueue.awaitSuccess();

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 20);
        Async async = context.async();
        eventBusSend(buildGetQueuesOperation("abc(.*"), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals(BAD_INPUT, message.result().body().getString(ERROR_TYPE));
            context.assertTrue(message.result().body().getString(MESSAGE).contains("Error while compile regex pattern"));
            async.complete();
        });
    }

    @Test
    public void getQueuesCount(TestContext context) {
        Async asyncEnqueue = context.async(100);
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        for (int i = 0; i < 100; i++) {
            eventBusSend(buildEnqueueOperation("queue" + i, "testItem"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                asyncEnqueue.countDown();
            });

        }
        asyncEnqueue.awaitSuccess();

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 100);
        Async async = context.async();
        eventBusSend(buildGetQueuesCountOperation(), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(100L, message.result().body().getLong(VALUE));
            async.complete();
        });
    }

    @Test
    public void getQueuesCountFiltered(TestContext context) {
        Async asyncEnqueue = context.async(50);
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        for (int i = 0; i < 50; i++) {
            eventBusSend(buildEnqueueOperation("queue" + i, "testItem"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                asyncEnqueue.countDown();
            });

        }
        asyncEnqueue.awaitSuccess();

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 50);
        Async async = context.async();
        eventBusSend(buildGetQueuesCountOperation("8"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(5L, message.result().body().getLong(VALUE));
            eventBusSend(buildGetQueuesCountOperation("abc"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                context.assertEquals(0L, message2.result().body().getLong(VALUE));
                async.complete();
            });
        });
    }

    @Test
    public void getQueuesCountFilteredInvalidPattern(TestContext context) {
        Async asyncEnqueue = context.async(50);
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        for (int i = 0; i < 50; i++) {
            eventBusSend(buildEnqueueOperation("queue" + i, "testItem"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                asyncEnqueue.countDown();
            });
        }
        asyncEnqueue.awaitSuccess();

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 50);
        Async async = context.async();
        eventBusSend(buildGetQueuesCountOperation("abc(.*"), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals(BAD_INPUT, message.result().body().getString(ERROR_TYPE));
            context.assertTrue(message.result().body().getString(MESSAGE).contains("Error while compile regex pattern"));
            async.complete();
        });
    }

    @Test
    public void getQueueItemsCount(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        String queue = "queue_1";
        for (int i = 0; i < 100; i++) {
            jedis.rpush(getQueuesRedisKeyPrefix() + queue, "testItem" + i);
        }
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(100L, jedis.llen(getQueuesRedisKeyPrefix() + queue));
        eventBusSend(buildGetQueueItemsCountOperation(queue), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(100L, message.result().body().getLong(VALUE));
            async.complete();
        });
    }

    @Test
    public void deleteAllQueueItemsNoItemsFound(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        final String queue = "queue1";
        eventBusSend(buildDeleteAllQueueItemsOperation(queue), message1 -> {
            context.assertEquals(OK, message1.result().body().getString(STATUS));
            context.assertEquals(0, message1.result().body().getInteger(VALUE));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
            async.complete();
        });
    }

    @Test
    public void deleteAllQueueItems(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        final String queue = "queue1";
        eventBusSend(buildEnqueueOperation(queue, "some_val"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
            eventBusSend(buildDeleteAllQueueItemsOperation(queue), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                context.assertEquals(1, message1.result().body().getInteger(VALUE));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                async.complete();
            });
        });
    }

    @Test
    public void deleteAllQueueItemsLegacyOperation(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        final String queue = "queue1";
        eventBusSend(buildEnqueueOperation(queue, "some_val"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);

            // use legacy operation without any 'unlock' configuration
            eventBusSend(buildOperation(QueueOperation.deleteAllQueueItems, new JsonObject().put(QUEUENAME, queue)), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                context.assertEquals(1, message1.result().body().getInteger(VALUE));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                async.complete();
            });
        });
    }

    @Test
    public void deleteAllQueueItemsWithLockLegacy(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        final String queue = "queue1";

        eventBusSend(buildPutLockOperation(queue, "geronimo"), event -> {
            assertLockExists(context, queue);
            eventBusSend(buildEnqueueOperation(queue, "some_val"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
                eventBusSend(buildDeleteAllQueueItemsOperation(queue), message1 -> {
                    context.assertEquals(OK, message1.result().body().getString(STATUS));
                    context.assertEquals(1, message1.result().body().getInteger(VALUE));
                    assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                    assertLockExists(context, queue); // check that lock still exists
                    async.complete();
                });
            });
        });
    }

    @Test
    public void deleteAllQueueItemsWithLockDontUnlock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        final String queue = "queue1";

        eventBusSend(buildPutLockOperation(queue, "geronimo"), event -> {
            assertLockExists(context, queue);
            eventBusSend(buildEnqueueOperation(queue, "some_val"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
                eventBusSend(buildDeleteAllQueueItemsOperation(queue, false), message1 -> {
                    context.assertEquals(OK, message1.result().body().getString(STATUS));
                    context.assertEquals(1, message1.result().body().getInteger(VALUE));
                    assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                    assertLockExists(context, queue); // check that lock still exists
                    async.complete();
                });
            });
        });
    }

    @Test
    public void deleteAllQueueItemsWithLockDoUnlock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        final String queue = "queue1";

        eventBusSend(buildPutLockOperation(queue, "geronimo"), event -> {
            assertLockExists(context, queue);
            eventBusSend(buildEnqueueOperation(queue, "some_val"), message -> {
                context.assertEquals(OK, message.result().body().getString(STATUS));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
                eventBusSend(buildDeleteAllQueueItemsOperation(queue, true), message1 -> {
                    context.assertEquals(OK, message1.result().body().getString(STATUS));
                    context.assertEquals(1, message1.result().body().getInteger(VALUE));
                    assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                    assertLockDoesNotExist(context, queue); // check that lock doesn't exist anymore
                    async.complete();
                });
            });
        });
    }

    @Test
    public void bulkDeleteQueues(TestContext context) {
        Async async = context.async();
        flushAll();

        eventBusSend(buildEnqueueOperation("q1", "q1_message"), e1 -> {
            eventBusSend(buildEnqueueOperation("q1", "q1_message"), e2 -> {
                eventBusSend(buildEnqueueOperation("q2", "q2_message"), e3 -> {
                    eventBusSend(buildEnqueueOperation("q3", "q3_message"), e4 -> {

                        assertQueuesCount(context, 3);
                        assertQueueItemsCount(context, "q1", 2);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 1);

                        eventBusSend(buildBulkDeleteQueuesOperation(new JsonArray()), m1 -> {
                            context.assertEquals(OK, m1.result().body().getString(STATUS));
                            context.assertEquals(0L, m1.result().body().getLong(VALUE));

                            assertQueuesCount(context, 3);
                            assertQueueItemsCount(context, "q1", 2);
                            assertQueueItemsCount(context, "q2", 1);
                            assertQueueItemsCount(context, "q3", 1);

                            eventBusSend(buildBulkDeleteQueuesOperation(new JsonArray().add("q1").add("q3")), m2 -> {
                                context.assertEquals(OK, m2.result().body().getString(STATUS));
                                context.assertEquals(2L, m2.result().body().getLong(VALUE));

                                assertQueuesCount(context, 1);
                                assertQueueItemsCount(context, "q1", 0);
                                assertQueueItemsCount(context, "q2", 1);
                                assertQueueItemsCount(context, "q3", 0);

                                // invalid operation setup
                                JsonObject operation = buildOperation(QueueOperation.bulkDeleteQueues, new JsonObject().put("abc", 123));
                                eventBusSend(operation, m3 -> {
                                    context.assertEquals(ERROR, m3.result().body().getString(STATUS));
                                    context.assertEquals("No queues to delete provided", m3.result().body().getString(MESSAGE));

                                    // not string values
                                    eventBusSend(buildBulkDeleteQueuesOperation(new JsonArray().add(111).add(222)), m4 -> {
                                        context.assertEquals(ERROR, m4.result().body().getString(STATUS));
                                        context.assertEquals("Queues must be string values", m4.result().body().getString(MESSAGE));
                                        async.complete();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }


    @Test
    public void addQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        eventBusSend(buildAddQueueItemOperation("queue2", "fooBar"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
            context.assertEquals("fooBar", jedis.lindex(getQueuesRedisKeyPrefix() + "queue2", 0));
            async.complete();
        });
    }

    @Test
    public void addQueueItemWithLegacyOperationName(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

        JsonObject op = new JsonObject();
        op.put(OPERATION, "addItem");
        op.put(PAYLOAD, new JsonObject().put(QUEUENAME, "queue2").put("buffer", "fooBar"));

        eventBusSend(op, message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
            context.assertEquals("fooBar", jedis.lindex(getQueuesRedisKeyPrefix() + "queue2", 0));
            async.complete();
        });
    }

    @Test
    public void getQueueItemsWithQueueSizeInformation(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        eventBusSend(buildAddQueueItemOperation("queue2", "fooBar"), message -> {
            eventBusSend(buildAddQueueItemOperation("queue2", "fooBar2"), message1 -> {
                eventBusSend(buildAddQueueItemOperation("queue2", "fooBar3"), message2 -> {
                    context.assertEquals(OK, message2.result().body().getString(STATUS));
                    eventBusSend(buildGetQueueItemsOperation("queue2", "2"), event -> {
                        context.assertEquals(OK, event.result().body().getString(STATUS));
                        context.assertEquals(2, event.result().body().getJsonArray(VALUE).size());
                        context.assertEquals(2, event.result().body().getJsonArray(INFO).getInteger(0));
                        context.assertEquals(3, event.result().body().getJsonArray(INFO).getInteger(1));
                        async.complete();
                    });
                });
            });
        });
    }

    @Test
    public void getQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        eventBusSend(buildGetQueueItemOperation("queue1", 0), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertNull(message.result().body().getString(VALUE));
            eventBusSend(buildAddQueueItemOperation("queue1", "fooBar"), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
                context.assertEquals("fooBar", jedis.lindex(getQueuesRedisKeyPrefix() + "queue1", 0));
                eventBusSend(buildGetQueueItemOperation("queue1", 0), message2 -> {
                    context.assertEquals(OK, message2.result().body().getString(STATUS));
                    context.assertEquals("fooBar", message2.result().body().getString("value"));
                    async.complete();
                });
            });
        });
    }

    @Test
    public void replaceQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildAddQueueItemOperation("queue1", "foo"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
            context.assertEquals("foo", jedis.lindex(getQueuesRedisKeyPrefix() + "queue1", 0));
            eventBusSend(buildReplaceQueueItemOperation("queue1", 0, "bar"), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                context.assertEquals("bar", jedis.lindex(getQueuesRedisKeyPrefix() + "queue1", 0));
                async.complete();
            });
        });
    }

    @Test
    public void replaceQueueItemWithLegacyOperationName(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildAddQueueItemOperation("queue1", "foo"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
            context.assertEquals("foo", jedis.lindex(getQueuesRedisKeyPrefix() + "queue1", 0));

            JsonObject op = new JsonObject();
            op.put(OPERATION, "replaceItem");
            op.put(PAYLOAD, new JsonObject().put(QUEUENAME, "queue1").put("index", 0).put("buffer", "bar"));

            eventBusSend(op, message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                context.assertEquals("bar", jedis.lindex(getQueuesRedisKeyPrefix() + "queue1", 0));
                async.complete();
            });
        });
    }

    @Test
    public void deleteQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix() + "queue1", 0);
        eventBusSend(buildDeleteQueueItemOperation("queue1", 0), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            eventBusSend(buildAddQueueItemOperation("queue1", "foo"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                assertKeyCount(context, getQueuesRedisKeyPrefix() + "queue1", 1);
                eventBusSend(buildDeleteQueueItemOperation("queue1", 0), message3 -> {
                    context.assertEquals(OK, message3.result().body().getString(STATUS));
                    assertKeyCount(context, getQueuesRedisKeyPrefix() + "queue1", 0);
                    async.complete();
                });
            });
        });
    }

    @Test
    public void getAllLocks(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildGetAllLocksOperation(), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonArray locksArray = message.result().body().getJsonObject(VALUE).getJsonArray("locks");
            context.assertNotNull(locksArray, "locks array should not be null");
            context.assertEquals(0, locksArray.size(), "locks array should be empty");
            eventBusSend(buildPutLockOperation("testLock", "geronimo"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                assertLockExists(context, "testLock");
                eventBusSend(buildGetAllLocksOperation(), message3 -> {
                    context.assertEquals(OK, message3.result().body().getString(STATUS));
                    JsonArray locksArray1 = message3.result().body().getJsonObject(VALUE).getJsonArray("locks");
                    context.assertNotNull(locksArray1, "locks array should not be null");
                    context.assertTrue(locksArray1.size() > 0, "locks array should not be empty");
                    if (locksArray1.size() > 0) {
                        String result = locksArray1.getString(0);
                        context.assertTrue(result.matches("testLock.*"));
                    }
                    async.complete();
                });
            });
        });
    }

    @Test
    public void getAllLocksFiltered(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildGetAllLocksOperation(), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonArray locksArray = message.result().body().getJsonObject(VALUE).getJsonArray("locks");
            context.assertNotNull(locksArray, "locks array should not be null");
            context.assertEquals(0, locksArray.size(), "locks array should be empty");
            eventBusSend(buildPutLockOperation("testLock", "geronimo"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                assertLockExists(context, "testLock");
                eventBusSend(buildPutLockOperation("abcLock", "geronimo"), message2a -> {
                    context.assertEquals(OK, message2a.result().body().getString(STATUS));
                    assertLockExists(context, "abcLock");
                    eventBusSend(buildGetAllLocksOperation(), message3 -> {
                        context.assertEquals(OK, message3.result().body().getString(STATUS));
                        JsonArray locksArray1 = message3.result().body().getJsonObject(VALUE).getJsonArray("locks");
                        context.assertNotNull(locksArray1, "locks array should not be null");
                        context.assertTrue(locksArray1.size() > 0, "locks array should not be empty");
                        if (locksArray1.size() > 0) {
                            String item0 = locksArray1.getString(0);
                            context.assertTrue(item0.matches("testLock.*"));
                            String item1 = locksArray1.getString(1);
                            context.assertTrue(item1.matches("abcLock.*"));
                        }
                        eventBusSend(buildGetAllLocksOperation("abc(.*)"), message4 -> {
                            context.assertEquals(OK, message4.result().body().getString(STATUS));
                            JsonArray locksArray2 = message4.result().body().getJsonObject(VALUE).getJsonArray("locks");
                            context.assertNotNull(locksArray2, "locks array should not be null");
                            context.assertEquals(1, locksArray2.size(), "locks array should have size 1");
                            String item0 = locksArray2.getString(0);
                            context.assertTrue(item0.matches("abcLock.*"));
                            async.complete();
                        });

                    });
                });

            });
        });
    }

    @Test
    public void putLock(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildPutLockOperation("queue1", "someuser"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertLockExists(context, "queue1");
            assertLockContent(context, "queue1", "someuser");
            async.complete();
        });
    }

    @Test
    public void putLockMissingRequestedBy(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getLocksRedisKey(), 0);
        assertLockDoesNotExist(context, "queue1");
        eventBusSend(buildOperation(QueueOperation.putLock, new JsonObject().put(QUEUENAME, "queue1")), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("Property '" + REQUESTED_BY + "' missing", message.result().body().getString(MESSAGE));
            assertKeyCount(context, getLocksRedisKey(), 0);
            assertLockDoesNotExist(context, "queue1");
            async.complete();
        });
    }

    @Test
    public void bulkPutLocks(TestContext context) {
        Async async = context.async();
        String user = "geronimo";
        flushAll();

        eventBusSend(buildBulkPutLocksOperation(new JsonArray(), user), m1 -> {
            context.assertEquals(ERROR, m1.result().body().getString(STATUS));
            context.assertEquals("No locks to put provided", m1.result().body().getString(MESSAGE));
            assertLocksCount(context, 0);

            eventBusSend(buildBulkPutLocksOperation(new JsonArray().add("q1").add(12345), user), m2 -> {
                context.assertEquals(ERROR, m2.result().body().getString(STATUS));
                context.assertEquals("Locks must be string values", m2.result().body().getString(MESSAGE));
                context.assertEquals(BAD_INPUT, m2.result().body().getString(ERROR_TYPE));
                assertLocksCount(context, 0);

                eventBusSend(buildBulkPutLocksOperation(new JsonArray().add("q1").add("q2").add("q3"), user), m3 -> {
                    context.assertEquals(OK, m3.result().body().getString(STATUS));
                    assertLocksCount(context, 3);
                    assertLockExists(context, "q1");
                    assertLockExists(context, "q2");
                    assertLockExists(context, "q3");

                    // invalid operation
                    JsonObject operation = buildOperation(QueueOperation.bulkPutLocks, new JsonObject().put("abc", 123));
                    eventBusSend(operation, m4 -> {
                        context.assertEquals(ERROR, m4.result().body().getString(STATUS));
                        context.assertEquals("No locks to put provided", m4.result().body().getString(MESSAGE));

                        assertLocksCount(context, 3);
                        assertLockExists(context, "q1");
                        assertLockExists(context, "q2");
                        assertLockExists(context, "q3");

                        async.complete();
                    });

                });
            });
        });
    }


    @Test
    public void getLock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getLocksRedisKey(), 0);
        eventBusSend(buildPutLockOperation("testLock1", "geronimo"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertLockExists(context, "testLock1");
            assertKeyCount(context, getLocksRedisKey(), 1);
            eventBusSend(buildGetLockOperation("testLock1"), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                assertLockExists(context, "testLock1");
                assertLockContent(context, "testLock1", "geronimo");
                async.complete();
            });
        });
    }

    @Test
    public void getNotExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getLocksRedisKey(), 0);
        eventBusSend(buildGetLockOperation("notExistingLock"), message -> {
            context.assertEquals(NO_SUCH_LOCK, message.result().body().getString(STATUS));
            assertKeyCount(context, getLocksRedisKey(), 0);
            assertLockDoesNotExist(context, "notExistingLock");
            async.complete();
        });
    }

    @Test
    public void testIgnoreCaseInOperationName(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getLocksRedisKey(), 0);

        JsonObject op = new JsonObject();
        op.put(OPERATION, "GeTLOcK");
        op.put(PAYLOAD, new JsonObject().put(QUEUENAME, "notExistingLock"));

        eventBusSend(op, message -> {
            context.assertEquals(NO_SUCH_LOCK, message.result().body().getString(STATUS));
            assertKeyCount(context, getLocksRedisKey(), 0);
            assertLockDoesNotExist(context, "notExistingLock");
            async.complete();
        });
    }

    @Test
    public void deleteNotExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildDeleteLockOperation("testLock1"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertLockDoesNotExist(context, "testLock1");
            async.complete();
        });
    }

    @Test
    public void deleteExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getLocksRedisKey(), 0);
        eventBusSend(buildPutLockOperation("testLock1", "someuser"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertLockExists(context, "testLock1");
            assertKeyCount(context, getLocksRedisKey(), 1);
            eventBusSend(buildDeleteLockOperation("testLock1"), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                assertLockDoesNotExist(context, "testLock1");
                assertKeyCount(context, getLocksRedisKey(), 0);
                async.complete();
            });
        });
    }

    @Test
    public void deleteAllLocks(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildGetAllLocksOperation(), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonArray locksArray = message.result().body().getJsonObject(VALUE).getJsonArray("locks");
            context.assertNotNull(locksArray, "locks array should not be null");
            context.assertEquals(0, locksArray.size(), "locks array should be empty");
            eventBusSend(buildPutLockOperation("testLock", "geronimo"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                assertLockExists(context, "testLock");
                eventBusSend(buildPutLockOperation("testLockWinnetou", "winnetou"), message3 -> {
                    context.assertEquals(OK, message3.result().body().getString(STATUS));
                    assertLockExists(context, "testLockWinnetou");
                    eventBusSend(buildDeleteAllLocksOperation(), message4 -> {
                        context.assertEquals(OK, message4.result().body().getString(STATUS));
                        context.assertEquals(2L, message4.result().body().getLong(VALUE));
                        assertLockDoesNotExist(context, "testLock");
                        assertLockDoesNotExist(context, "testLockWinnetou");
                        eventBusSend(buildGetAllLocksOperation(), message5 -> {
                            context.assertEquals(OK, message5.result().body().getString(STATUS));
                            JsonArray locksArray1 = message5.result().body().getJsonObject(VALUE).getJsonArray("locks");
                            context.assertNotNull(locksArray1, "locks array should not be null");
                            context.assertEquals(0, locksArray1.size(), "locks array should be empty");
                            async.complete();
                        });
                    });
                });
            });
        });
    }

    @Test
    public void bulkDeleteLocks(TestContext context) {
        Async async = context.async();
        String user = "geronimo";
        flushAll();
        eventBusSend(buildPutLockOperation("q1", user), p1 -> {
            eventBusSend(buildPutLockOperation("q2", user), p2 -> {
                eventBusSend(buildPutLockOperation("q3", user), p3 -> {
                    assertLockExists(context, "q1");
                    assertLockExists(context, "q2");
                    assertLockExists(context, "q3");

                    eventBusSend(buildBulkDeleteLocksOperation(new JsonArray()), m1 -> {
                        context.assertEquals(OK, m1.result().body().getString(STATUS));
                        context.assertEquals(0L, m1.result().body().getLong(VALUE));
                        assertLockExists(context, "q1");
                        assertLockExists(context, "q2");
                        assertLockExists(context, "q3");

                        eventBusSend(buildBulkDeleteLocksOperation(new JsonArray().add("q1").add("q3")), m2 -> {
                            context.assertEquals(OK, m2.result().body().getString(STATUS));
                            context.assertEquals(2L, m2.result().body().getLong(VALUE));
                            assertLockDoesNotExist(context, "q1");
                            assertLockDoesNotExist(context, "q3");
                            assertLockExists(context, "q2");

                            //delete the same locks again
                            eventBusSend(buildBulkDeleteLocksOperation(new JsonArray().add("q1").add("q3")), m3 -> {
                                context.assertEquals(OK, m3.result().body().getString(STATUS));
                                context.assertEquals(0L, m3.result().body().getLong(VALUE));
                                assertLockDoesNotExist(context, "q1");
                                assertLockDoesNotExist(context, "q3");
                                assertLockExists(context, "q2");

                                eventBusSend(buildBulkDeleteLocksOperation(new JsonArray().add(123).add("q2")), m4 -> {
                                    //NOTE vertx 4 doesn't force the type anymore.
                                    context.assertEquals(OK, m4.result().body().getString(STATUS));
                                    //context.assertEquals(BAD_INPUT, m4.result().body().getString(ERROR_TYPE));
                                    //context.assertEquals("Locks must be string values", m4.result().body().getString(MESSAGE));
                                    assertLockDoesNotExist(context, "q1");
                                    assertLockDoesNotExist(context, "q3");
                                    assertLockDoesNotExist(context, "q2");

                                    async.complete();
                                });
                            });
                        });
                    });
                });
            });
        });
    }

    @Test
    public void bulkDeleteLocksInvalidOperationSetup(TestContext context) {
        Async async = context.async();
        String user = "geronimo";
        flushAll();
        eventBusSend(buildPutLockOperation("q1", user), p1 -> {
            eventBusSend(buildPutLockOperation("q2", user), p2 -> {
                eventBusSend(buildPutLockOperation("q3", user), p3 -> {
                    assertLockExists(context, "q1");
                    assertLockExists(context, "q2");
                    assertLockExists(context, "q3");

                    JsonObject op = buildOperation(QueueOperation.bulkDeleteLocks);
                    op.put(PAYLOAD, new JsonObject());

                    eventBusSend(op, message -> {
                        context.assertEquals(ERROR, message.result().body().getString(STATUS));
                        context.assertEquals("No locks to delete provided", message.result().body().getString(MESSAGE));
                        async.complete();
                    });
                });
            });
        });
    }


    @Test
    public void checkLimit(TestContext context) {
        Async async = context.async();
        flushAll();
        for (int i = 0; i < 250; i++) {
            jedis.rpush(getQueuesRedisKeyPrefix() + "testLock1", "testItem" + i);
        }
        eventBusSend(buildGetQueueItemsOperation("testLock1", "178"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(178, message.result().body().getJsonArray(VALUE).size());
            context.assertEquals(178, message.result().body().getJsonArray(INFO).getInteger(0));
            context.assertEquals(250, message.result().body().getJsonArray(INFO).getInteger(1));
            async.complete();
        });
    }

    @Test
    public void getQueueRescheduleRefreshPeriodWhileFailureCountIncreasedUntilExceedMaxRetryInterval(TestContext context) {
        final String queue = "queue1";

        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong when failure count is 0.");
        
        // send message fail
        context.assertEquals(2, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 1.");
        
        // send message fail
        context.assertEquals(7, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 2.");
        
        // send message fail
        context.assertEquals(12, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 3.");
        
        // send message fail
        context.assertEquals(17, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 4.");
        
        // send message fail
        context.assertEquals(22, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 5.");
        
        // send message fail
        context.assertEquals(27, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 6.");
        
        // send message fail
        context.assertEquals(32, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 7.");
        
        // send message fail
        context.assertEquals(37, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 8.");
        
        // send message fail
        context.assertEquals(42, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 9.");
        
        // send message fail
        context.assertEquals(47, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 10.");
        
        // send message fail
        context.assertEquals(52, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 11.");

        // already reach the max retry interval

        // send message fail
        context.assertEquals(52, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong when failure count is 12.");
    }

    @Test
    public void getQueueRescheduleRefreshPeriodAfterProcessMessageFail(TestContext context) {
        final String queue = "queue1";

        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");

        // send message fail
        context.assertEquals(2, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");
        
        // send message fail
        context.assertEquals(7, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");
    }

    @Test
    public void getQueueRescheduleRefreshPeriodAfterProcessMessageSuccess(TestContext context) {
        final String queue = "queue1";
        
        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");
        
        // send message fail
        context.assertEquals(2, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");

        // send message success
        context.assertEquals(0, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");
    }
    
    @Test
    public void getRescheduleRefreshPeriodOfUnknownQueue(TestContext context) {
        final String queue = "unknownqueue";

        // send message success (reset the failure count)
        context.assertEquals(0, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, true), "The retry interval is wrong");

        // send message fail
        context.assertEquals(2, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");

        // send message fail
        // still use the default refresh period
        context.assertEquals(2, redisQues.updateQueueFailureCountAndGetRetryInterval(queue, false), "The retry interval is wrong");
    }

    @Test
    public void enqueueWithLimits(TestContext context) {
        Async async = context.async();
        flushAll();

        eventBusSend(buildEnqueueOperation("limited-queue-1.test", "message_1-1"), e1 -> {
            eventBusSend(buildEnqueueOperation("limited-queue-1.test", "message_1-2"), e2 -> {
                eventBusSend(buildEnqueueOperation("limited-queue-1.test", "message_1-3"), e3 -> {
                    eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-1"), e4 -> {
                        eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-2"), e5 -> {
                            eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-3"), e6 -> {
                                eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-4"), e7 -> {
                                    eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-5"), e8 -> {
                                        eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-6"), e9 -> {
                                            eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-7"), e10 -> {
                                                eventBusSend(buildEnqueueOperation("limited-queue-4.test", "message_4-8"), e11 -> {
                                                    assertQueuesCount(context, 2);
                                                    // wait few seconds.
                                                    try {
                                                        Thread.sleep(5000);
                                                    } catch (InterruptedException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                    assertQueueItemsCount(context, "limited-queue-1.test", 1);
                                                    assertQueueItemsCount(context, "limited-queue-4.test", 4);
                                                    eventBusSend(buildGetQueueItemsOperation("limited-queue-1.test", null), event -> {
                                                        context.assertEquals(OK, event.result().body().getString(STATUS));
                                                        context.assertEquals(1, event.result().body().getJsonArray(VALUE).size());
                                                        context.assertEquals("message_1-3", event.result().body().getJsonArray(VALUE).getString(0));
                                                        context.assertEquals(1, event.result().body().getJsonArray(INFO).getInteger(0));
                                                        context.assertEquals(1, event.result().body().getJsonArray(INFO).getInteger(1));
                                                        eventBusSend(buildGetQueueItemsOperation("limited-queue-4.test", null), event2 -> {
                                                            context.assertEquals(OK, event2.result().body().getString(STATUS));
                                                            context.assertEquals(4, event2.result().body().getJsonArray(VALUE).size());
                                                            context.assertEquals("message_4-5", event2.result().body().getJsonArray(VALUE).getString(0));
                                                            context.assertEquals("message_4-6", event2.result().body().getJsonArray(VALUE).getString(1));
                                                            context.assertEquals("message_4-7", event2.result().body().getJsonArray(VALUE).getString(2));
                                                            context.assertEquals("message_4-8", event2.result().body().getJsonArray(VALUE).getString(3));
                                                            async.complete();
                                                        });
                                                    });
                                                });
                                            });
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


    @Test
    public void addQueueItemWithLimits(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildAddQueueItemOperation("limited-queue-1.test", "message_1-1"), e1 -> {
            eventBusSend(buildAddQueueItemOperation("limited-queue-1.test", "message_1-2"), e2 -> {
                eventBusSend(buildAddQueueItemOperation("limited-queue-1.test", "message_1-3"), e3 -> {
                    eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-1"), e4 -> {
                        eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-2"), e5 -> {
                            eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-3"), e6 -> {
                                eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-4"), e7 -> {
                                    eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-5"), e8 -> {
                                        eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-6"), e9 -> {
                                            eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-7"), e10 -> {
                                                eventBusSend(buildAddQueueItemOperation("limited-queue-4.test", "message_4-8"), e11 -> {
                                                    assertQueuesCount(context, 2);
                                                    assertQueueItemsCount(context, "limited-queue-1.test", 1);
                                                    assertQueueItemsCount(context, "limited-queue-4.test", 4);
                                                    eventBusSend(buildGetQueueItemsOperation("limited-queue-1.test", null), event -> {
                                                        context.assertEquals(OK, event.result().body().getString(STATUS));
                                                        context.assertEquals(1, event.result().body().getJsonArray(VALUE).size());
                                                        context.assertEquals("message_1-3", event.result().body().getJsonArray(VALUE).getString(0));
                                                        context.assertEquals(1, event.result().body().getJsonArray(INFO).getInteger(0));
                                                        context.assertEquals(1, event.result().body().getJsonArray(INFO).getInteger(1));
                                                        eventBusSend(buildGetQueueItemsOperation("limited-queue-4.test", null), event2 -> {
                                                            context.assertEquals(OK, event2.result().body().getString(STATUS));
                                                            context.assertEquals(4, event2.result().body().getJsonArray(VALUE).size());
                                                            context.assertEquals("message_4-5", event2.result().body().getJsonArray(VALUE).getString(0));
                                                            context.assertEquals("message_4-6", event2.result().body().getJsonArray(VALUE).getString(1));
                                                            context.assertEquals("message_4-7", event2.result().body().getJsonArray(VALUE).getString(2));
                                                            context.assertEquals("message_4-8", event2.result().body().getJsonArray(VALUE).getString(3));
                                                            async.complete();
                                                        });
                                                    });
                                                });
                                            });
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

    private void assertEnqueueCounts(TestContext context, double successCount, double failCount){
        context.assertEquals(successCount, enqueueCounterSuccess.count(), "Success enqueue count is wrong");
        context.assertEquals(failCount, enqueueCounterFail.count(), "Failed enqueue count is wrong");
    }
}
