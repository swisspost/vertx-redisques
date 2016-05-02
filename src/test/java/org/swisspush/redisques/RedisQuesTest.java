package org.swisspush.redisques;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

/**
 * Class RedisQueueBrowserTest.
 *
 * @author baldim, webermarca
 */
public class RedisQuesTest extends AbstractTestCase {

    @Test
    public void enqueueWithQueueProcessor(TestContext context) throws Exception {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, 0);
        final JsonObject operation = enqueueOperation("queue1", "hello");
        vertx.eventBus().consumer("digest-queue1", (Handler<Message<String>>) event -> {});
        eventBusSend(operation, reply -> {
            context.assertEquals(OK, reply.result().body().getString(STATUS));
            operation.put("message", "STOP");
            eventBusSend(operation, reply1 -> {
                context.assertEquals(OK, reply1.result().body().getString(STATUS));
                async.complete();
            });
        });
    }

    @Test
    public void testMore(TestContext context) throws Exception {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, 0);
        for (int i = 0; i < NUM_QUEUES; i++) {
            new Sender(context, async, "queue_" + i).send(null);
        }
    }

    @Test
    public void enqueue(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        eventBusSend(enqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals("helloEnqueue", jedis.lindex("redisques:queues:queueEnqueue", 0));
            assertKeyCount(context, QUEUES_PREFIX, 1);
            async.complete();
        });
    }

    @Test
    public void getListRange(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildOperation(Operation.getListRange, new JsonObject().put(QUEUENAME, "queue1")), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(0, message.result().body().getJsonArray(VALUE).size());
            eventBusSend(enqueueOperation("queue1", "a_queue_item"), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                assertKeyCount(context, QUEUES_PREFIX, 1);
                eventBusSend(buildOperation(Operation.getListRange, new JsonObject().put(QUEUENAME, "queue1")), event -> {
                    context.assertEquals(OK, event.result().body().getString(STATUS));
                    context.assertEquals(1, event.result().body().getJsonArray(VALUE).size());
                    context.assertEquals("a_queue_item", event.result().body().getJsonArray(VALUE).getString(0));
                    async.complete();
                });
            });
        });
    }

    @Test
    public void deleteAllQueueItems(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        final String queue = "queue1";
        eventBusSend(enqueueOperation(queue, "some_val"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, QUEUES_PREFIX, 1);
            eventBusSend(buildOperation(Operation.deleteAllQueueItems, new JsonObject().put(QUEUENAME, queue)), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                assertKeyCount(context, QUEUES_PREFIX, 0);
                async.complete();
            });
        });
    }

    @Test
    public void addItem(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        eventBusSend(buildOperation(Operation.addItem, new JsonObject().put(QUEUENAME, "queue2").put(BUFFER, "fooBar")), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, QUEUES_PREFIX, 1);
            context.assertEquals("fooBar", jedis.lindex(QUEUES_PREFIX + "queue2", 0));
            async.complete();
        });
    }

    @Test
    public void getListRangeWithQueueSizeInformation(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        eventBusSend(buildOperation(Operation.addItem, new JsonObject().put(QUEUENAME, "queue2").put(BUFFER, "fooBar")), message -> {
            eventBusSend(buildOperation(Operation.addItem, new JsonObject().put(QUEUENAME, "queue2").put(BUFFER, "fooBar2")), message1 -> {
                eventBusSend(buildOperation(Operation.addItem, new JsonObject().put(QUEUENAME, "queue2").put(BUFFER, "fooBar3")), message2 -> {
                    context.assertEquals(OK, message2.result().body().getString(STATUS));
                    eventBusSend(buildOperation(Operation.getListRange, new JsonObject().put(QUEUENAME, "queue2").put(LIMIT, "2")), event -> {
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
    public void getItem(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        eventBusSend(buildOperation(Operation.getItem, new JsonObject().put(QUEUENAME, "queue1").put(INDEX, 0)), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertNull(message.result().body().getString(VALUE));
            eventBusSend(buildOperation(Operation.addItem, new JsonObject().put(QUEUENAME, "queue1").put(BUFFER, "fooBar")), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                assertKeyCount(context, QUEUES_PREFIX, 1);
                context.assertEquals("fooBar", jedis.lindex(QUEUES_PREFIX + "queue1", 0));
                eventBusSend(buildOperation(Operation.getItem, new JsonObject().put(QUEUENAME, "queue1").put(INDEX, 0)), message2 -> {
                    context.assertEquals(OK, message2.result().body().getString(STATUS));
                    context.assertEquals("fooBar", message2.result().body().getString("value"));
                    async.complete();
                });
            });
        });
    }

    @Test
    public void replaceItem(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildOperation(Operation.addItem, new JsonObject().put(QUEUENAME, "queue1").put(BUFFER, "foo")), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            assertKeyCount(context, QUEUES_PREFIX, 1);
            context.assertEquals("foo", jedis.lindex(QUEUES_PREFIX + "queue1", 0));
            eventBusSend(buildOperation(Operation.replaceItem, new JsonObject().put(QUEUENAME, "queue1").put(BUFFER, "bar").put(INDEX, 0)), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                context.assertEquals("bar", jedis.lindex(QUEUES_PREFIX + "queue1", 0));
                async.complete();
            });
        });
    }

    @Test
    public void deleteItem(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, QUEUES_PREFIX + "queue1", 0);
        eventBusSend(buildOperation(Operation.deleteItem, new JsonObject().put(QUEUENAME, "queue1").put(INDEX, 0)), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            eventBusSend(buildOperation(Operation.addItem, new JsonObject().put(QUEUENAME, "queue1").put(BUFFER, "foo")), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                assertKeyCount(context, QUEUES_PREFIX + "queue1", 1);
                eventBusSend(buildOperation(Operation.deleteItem, new JsonObject().put(QUEUENAME, "queue1").put(INDEX, 0)), message3 -> {
                    context.assertEquals(OK, message3.result().body().getString(STATUS));
                    assertKeyCount(context, QUEUES_PREFIX + "queue1", 0);
                    async.complete();
                });
            });
        });
    }

    @Test
    public void getAllLocks(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildOperation(Operation.getAllLocks, new JsonObject()), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            JsonArray locksArray = message.result().body().getJsonObject(VALUE).getJsonArray("locks");
            context.assertNotNull(locksArray, "locks array should not be null");
            context.assertEquals(0, locksArray.size(), "locks array should be empty");
            eventBusSend(putLockOperation("testLock", "geronimo"), message2 -> {
                context.assertEquals(OK, message2.result().body().getString(STATUS));
                context.assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock"));
                eventBusSend(buildOperation(Operation.getAllLocks, new JsonObject()), message3 -> {
                    context.assertEquals(OK, message3.result().body().getString(STATUS));
                    JsonArray locksArray1 = message3.result().body().getJsonObject(VALUE).getJsonArray("locks");
                    context.assertNotNull(locksArray1, "locks array should not be null");
                    context.assertTrue(locksArray1.size() > 0, "locks array should not be empty");
                    if(locksArray1.size() > 0) {
                        String result = locksArray1.getString(0);
                        context.assertTrue(result.matches("testLock.*"));
                    }
                    async.complete();
                });
            });
        });
    }

    @Test
    public void putLock(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(putLockOperation("queue1", "someuser"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertTrue(jedis.hexists(REDISQUES_LOCKS, "queue1"));
            assertLockContent(context, "queue1", "someuser");
            async.complete();
        });
    }

    @Test
    public void putLockMissingRequestedBy(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, REDISQUES_LOCKS, 0);
        context.assertFalse(jedis.hexists(REDISQUES_LOCKS, "queue1"));
        eventBusSend(buildOperation(Operation.putLock, new JsonObject().put(QUEUENAME, "queue1")), message -> {
            context.assertEquals(ERROR, message.result().body().getString(STATUS));
            context.assertEquals("Property '"+REQUESTEDBY+"' missing", message.result().body().getString(MESSAGE));
            assertKeyCount(context, REDISQUES_LOCKS, 0);
            context.assertFalse(jedis.hexists(REDISQUES_LOCKS, "queue1"));
            async.complete();
        });
    }

    @Test
    public void getLock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, REDISQUES_LOCKS, 0);
        eventBusSend(putLockOperation("testLock1", "geronimo"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
            assertKeyCount(context, REDISQUES_LOCKS, 1);
            eventBusSend(buildOperation(Operation.getLock, new JsonObject().put(QUEUENAME, "testLock1")), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                context.assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
                assertLockContent(context, "testLock1", "geronimo");
                async.complete();
            });
        });
    }

    @Test
    public void getNotExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, REDISQUES_LOCKS, 0);
        eventBusSend(buildOperation(Operation.getLock, new JsonObject().put(QUEUENAME, "notExistingLock")), message -> {
            context.assertEquals("No such lock", message.result().body().getString(STATUS));
            assertKeyCount(context, REDISQUES_LOCKS, 0);
            context.assertFalse(jedis.hexists(REDISQUES_LOCKS, "notExistingLock"));
            async.complete();
        });
    }

    @Test
    public void deleteNotExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildOperation(Operation.deleteLock, new JsonObject().put(QUEUENAME, "testLock1")), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertFalse(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
            async.complete();
        });
    }

    @Test
    public void deleteExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, REDISQUES_LOCKS, 0);
        eventBusSend(putLockOperation("testLock1", "someuser"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
            assertKeyCount(context, REDISQUES_LOCKS, 1);
            eventBusSend(buildOperation(Operation.deleteLock, new JsonObject().put(QUEUENAME, "testLock1")), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                context.assertFalse(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
                assertKeyCount(context, REDISQUES_LOCKS, 0);
                async.complete();
            });
        });
    }

    @Test
    public void checkLimit(TestContext context) {
        Async async = context.async();
        flushAll();
        for (int i = 0; i < 250; i++) {
            jedis.rpush("redisques:queues:testLock1", "testItem"+i);
        }
        eventBusSend(buildOperation(Operation.getListRange, new JsonObject().put(QUEUENAME, "testLock1").put("limit", "178")), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            context.assertEquals(178, message.result().body().getJsonArray(VALUE).size());
            async.complete();
        });
    }

    private void assertLockContent(TestContext context, String queuename, String expectedRequestedByValue){
        String item = jedis.hget(REDISQUES_LOCKS, queuename);
        context.assertNotNull(item);
        if(item != null){
            JsonObject lockInfo = new JsonObject(item);
            context.assertNotNull(lockInfo.getString(REQUESTEDBY), "Property '"+REQUESTEDBY+"' missing");
            context.assertNotNull(lockInfo.getLong(TIMESTAMP), "Property '"+TIMESTAMP+"' missing");
            context.assertEquals(expectedRequestedByValue, lockInfo.getString(REQUESTEDBY), "Property '"+REQUESTEDBY+"' has wrong value");
        }
    }

    private void eventBusSend(JsonObject operation, Handler<AsyncResult<Message<JsonObject>>> handler){
        vertx.eventBus().send("redisques", operation, handler);
    }
}
