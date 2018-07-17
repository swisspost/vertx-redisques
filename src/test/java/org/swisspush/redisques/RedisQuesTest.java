package org.swisspush.redisques;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.util.RedisquesConfiguration;
import redis.clients.jedis.Jedis;

import java.util.Optional;

import static java.util.Optional.of;
import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class RedisQueueBrowserTest.
 *
 * @author baldim, webermarca
 */
public class RedisQuesTest extends AbstractTestCase {

    private RedisQues redisQues;
    
    @Rule
    public Timeout rule = Timeout.seconds(5);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();

        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .redisEncoding("ISO-8859-1")
                .refreshPeriod(2)
                .maxSlowDown(52)
                .build()
                .asJsonObject();

        redisQues = new RedisQues();
        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
        }));
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
            context.assertEquals(configuration.getString("redis-prefix"), "redisques:");
            context.assertEquals(configuration.getString("redisEncoding"), "ISO-8859-1");

            context.assertEquals(configuration.getInteger("checkInterval"), 60);
            context.assertEquals(configuration.getInteger("refresh-period"), 2);
            context.assertEquals(configuration.getInteger("processorTimeout"), 240000);
            context.assertEquals(configuration.getLong("processorDelayMax"), 0L);

            context.assertFalse(configuration.getBoolean("httpRequestHandlerEnabled"));
            context.assertEquals(configuration.getInteger("httpRequestHandlerPort"), 7070);
            context.assertEquals(configuration.getString("httpRequestHandlerPrefix"), "/queuing");
            context.assertEquals(configuration.getString("httpRequestHandlerUserHeader"), "x-rp-usr");

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
            async.complete();
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
            async.complete();
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
        eventBusSend(buildGetQueuesOperation(Optional.of("3")), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonArray queuesArray = message.result().body().getJsonObject(VALUE).getJsonArray("queues");
            context.assertEquals(2, queuesArray.size());
            context.assertTrue(queuesArray.contains("queue3"), "item queue1 expected to be in result");
            context.assertTrue(queuesArray.contains("queue13"), "item queue11 expected to be in result");

            eventBusSend(buildGetQueuesOperation(Optional.of("abc")), message2 -> {
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
        eventBusSend(buildGetQueuesOperation(Optional.of("abc(.*")), message -> {
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
        eventBusSend(buildGetQueuesCountOperation(Optional.of("8")), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(5L, message.result().body().getLong(VALUE));
            eventBusSend(buildGetQueuesCountOperation(Optional.of("abc")), message2 -> {
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
        eventBusSend(buildGetQueuesCountOperation(Optional.of("abc(.*")), message -> {
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
                        eventBusSend(buildGetAllLocksOperation(of("abc(.*)")), message4 -> {
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
                                    context.assertEquals(ERROR, m4.result().body().getString(STATUS));
                                    context.assertEquals(BAD_INPUT, m4.result().body().getString(ERROR_TYPE));
                                    context.assertEquals("Locks must be string values", m4.result().body().getString(MESSAGE));
                                    assertLockDoesNotExist(context, "q1");
                                    assertLockDoesNotExist(context, "q3");
                                    assertLockExists(context, "q2");

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
    public void getQueueRescheduleRefreshPeriodWhileFailureCountIncreased(TestContext context) {
        final String queue = "queue1";

        Async async = context.async();
        flushAll();
        
        redisQues.resetQueueProcessMessageFailureCount(queue, asyncResult1 -> {
            redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult2 -> {
                context.assertEquals(2, asyncResult2.result(), "The reschedule refresh period is wrong when failure count is 0.");

                redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult3 -> {
                    redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult4 -> {
                        context.assertEquals(7, asyncResult4.result(), "The reschedule refresh period is wrong when failure count is 1.");

                        redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult5 -> {
                            redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult6 -> {
                                context.assertEquals(12, asyncResult6.result(), "The reschedule refresh period is wrong when failure count is 2.");

                                redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult7 -> {
                                    redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult8 -> {
                                        context.assertEquals(17, asyncResult8.result(), "The reschedule refresh period is wrong when failure count is 3.");

                                        redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult9 -> {
                                            redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult10 -> {
                                                context.assertEquals(22, asyncResult10.result(), "The reschedule refresh period is wrong when failure count is 4.");

                                                redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult11 -> {
                                                    redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult12 -> {
                                                        context.assertEquals(27, asyncResult12.result(), "The reschedule refresh period is wrong when failure count is 5.");

                                                        redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult13 -> {
                                                            redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult14 -> {
                                                                context.assertEquals(32, asyncResult14.result(), "The reschedule refresh period is wrong when failure count is 6.");

                                                                redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult15 -> {
                                                                    redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult16 -> {
                                                                        context.assertEquals(37, asyncResult16.result(), "The reschedule refresh period is wrong when failure count is 7.");

                                                                        redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult17 -> {
                                                                            redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult18 -> {
                                                                                context.assertEquals(42, asyncResult18.result(), "The reschedule refresh period is wrong when failure count is 8.");

                                                                                redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult19 -> {
                                                                                    redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult20 -> {
                                                                                        context.assertEquals(47, asyncResult20.result(), "The reschedule refresh period is wrong when failure count is 9.");

                                                                                        redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult21 -> {
                                                                                            redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult22 -> {
                                                                                                context.assertEquals(52, asyncResult22.result(), "The reschedule refresh period is wrong when failure count is 10.");

                                                                                                // already reach the max slow down
                                                                                                redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult23 -> {
                                                                                                    redisQues.getQueueRescheduleRefreshPeriod(queue, asyncResult24 -> {
                                                                                                        context.assertEquals(52, asyncResult24.result(), "The reschedule refresh period is wrong when failure count is 11.");

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
    public void getQueueFailureCountWhenProcessMessageWithTimeoutFail(TestContext context) {
        final String queue = "queue1";
        String payload = "";

        Async async = context.async();
        flushAll();
        
        redisQues.resetQueueProcessMessageFailureCount(queue, asyncResult1 -> {
            redisQues.processMessageWithTimeout(queue, payload, sendResult -> {
                context.assertTrue(!sendResult.success, "The processed message is not success.");

                redisQues.getQueueProcessMessageFailureCount(queue, asyncResult2 -> {
                    context.assertEquals(1, asyncResult2.result(), "The failure count is wrong.");
                    async.complete();
                });
            });
        });
    }

    //FIXME: Cannot mock the event bus to send successfully
    @Test
    @Ignore
    public void getQueueFailureCountWhenProcessMessageWithTimeoutSuccess(TestContext context) {
        final String queue = "queue1";
        String payload = "{}";

        Async async = context.async();
        flushAll();

        redisQues.resetQueueProcessMessageFailureCount(queue, asyncResult1 -> {
            redisQues.getQueueProcessMessageFailureCount(queue, asyncResult2 -> {
                context.assertEquals(0, asyncResult2.result(), "The failure count is wrong");
                
                redisQues.increaseQueueProcessMessageFailureCount(queue, asyncResult3 -> {
                    redisQues.getQueueProcessMessageFailureCount(queue, asyncResult4 -> {
                        context.assertEquals(1, asyncResult4.result(), "The failure count is wrong");

                        redisQues.processMessageWithTimeout(queue, payload, sendResult5 -> {
                            
                            context.assertTrue(sendResult5.success, "The sending is not success");

                            redisQues.getQueueProcessMessageFailureCount(queue, asyncResult6 -> {
                                context.assertEquals(0, asyncResult6.result(), "The failure count is wrong");
                                async.complete();
                            });
                        });
                    });
                });
            });
        });
    }
}
