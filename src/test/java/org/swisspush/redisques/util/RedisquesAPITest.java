package org.swisspush.redisques.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Tests for {@link RedisquesAPI} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class RedisquesAPITest {

    public static final String QUEUENAME = "queuename";
    public static final String PROCESSOR_DELAY_MAX = "processorDelayMax";
    public static final String REQUESTED_BY = "requestedBy";
    public static final String INDEX = "index";
    public static final String LIMIT = "limit";
    public static final String BUFFER = "buffer";

    @Test
    public void testQueueOperationFromString(TestContext context){
        context.assertNull(QueueOperation.fromString("abc"));
        context.assertNull(QueueOperation.fromString("dummy"));
        context.assertNull(QueueOperation.fromString("doEnqueueThisItemPlease"));
        context.assertNull(QueueOperation.fromString(""));
        context.assertNull(QueueOperation.fromString(null));

        context.assertEquals(QueueOperation.check, QueueOperation.fromString("check"));
        context.assertEquals(QueueOperation.check, QueueOperation.fromString("CHECK"));

        context.assertEquals(QueueOperation.getAllLocks, QueueOperation.fromString("getAllLocks"));
        context.assertEquals(QueueOperation.getAllLocks, QueueOperation.fromString("getallLOCKS"));

        context.assertEquals(QueueOperation.getQueueItems, QueueOperation.fromString("getListRange")); // legacy
        context.assertEquals(QueueOperation.getQueueItems, QueueOperation.fromString("GETLISTRANGE")); // legacy
        context.assertEquals(QueueOperation.getQueueItems, QueueOperation.fromString("getQueueItems"));

        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addItem")); // legacy
        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addITEM")); // legacy
        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addQueueItem"));
        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addQUEUEItem"));

        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("deleteItem")); // legacy
        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("DELETEItem")); // legacy
        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("deleteQueueItem"));
        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("DELETEQueueItem"));

        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("getItem")); // legacy
        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("getitem")); // legacy
        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("getQueueItem"));
        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("GETQueUEItem"));

        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("replaceItem")); // legacy
        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("replACeIteM")); // legacy
        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("replaceQueueItem"));
        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("REPLACEQUEUEITEM"));

        context.assertEquals(QueueOperation.getQueues, QueueOperation.fromString("getQueues"));
        context.assertEquals(QueueOperation.getQueuesCount, QueueOperation.fromString("getQueuesCount"));
        context.assertEquals(QueueOperation.getQueueItemsCount, QueueOperation.fromString("getQueueItemsCount"));
    }

    @Test
    public void testLegacyName(TestContext context){
        context.assertTrue(QueueOperation.getQueueItems.hasLegacyName());
        context.assertTrue(QueueOperation.addQueueItem.hasLegacyName());
        context.assertTrue(QueueOperation.deleteQueueItem.hasLegacyName());
        context.assertTrue(QueueOperation.getQueueItem.hasLegacyName());
        context.assertTrue(QueueOperation.replaceQueueItem.hasLegacyName());

        context.assertEquals("getListRange", QueueOperation.getQueueItems.getLegacyName());
        context.assertEquals("addItem", QueueOperation.addQueueItem.getLegacyName());
        context.assertEquals("deleteItem", QueueOperation.deleteQueueItem.getLegacyName());
        context.assertEquals("getItem", QueueOperation.getQueueItem.getLegacyName());
        context.assertEquals("replaceItem", QueueOperation.replaceQueueItem.getLegacyName());

        context.assertFalse(QueueOperation.enqueue.hasLegacyName());
        context.assertFalse(QueueOperation.check.hasLegacyName());
        context.assertFalse(QueueOperation.reset.hasLegacyName());
        context.assertFalse(QueueOperation.stop.hasLegacyName());
        context.assertFalse(QueueOperation.deleteAllQueueItems.hasLegacyName());
        context.assertFalse(QueueOperation.getAllLocks.hasLegacyName());
        context.assertFalse(QueueOperation.putLock.hasLegacyName());
        context.assertFalse(QueueOperation.getLock.hasLegacyName());
        context.assertFalse(QueueOperation.deleteLock.hasLegacyName());
        context.assertFalse(QueueOperation.getQueues.hasLegacyName());
        context.assertFalse(QueueOperation.getQueuesCount.hasLegacyName());
        context.assertFalse(QueueOperation.getQueueItemsCount.hasLegacyName());
        context.assertFalse(QueueOperation.getConfiguration.hasLegacyName());
    }

    @Test
    public void testBuildEnqueueOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildEnqueueOperation("my_queue_name", "my_queue_value");
        JsonObject expected = buildExpectedJsonObject("enqueue", new JsonObject().put(QUEUENAME, "my_queue_name"), "my_queue_value");
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildLockedEnqueueOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildLockedEnqueueOperation("my_queue_name", "my_queue_value", "lockUser");
        JsonObject expected = buildExpectedJsonObject("lockedEnqueue",
                new JsonObject().put(QUEUENAME, "my_queue_name").put(REQUESTED_BY, "lockUser"), "my_queue_value");
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildGetQueueItemsOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetQueueItemsOperation("my_queue_name", "99");
        JsonObject expected = buildExpectedJsonObject("getQueueItems", new JsonObject()
                .put(QUEUENAME, "my_queue_name")
                .put(LIMIT, "99"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildAddQueueItemOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildAddQueueItemOperation("my_queue_name", "buffer_value");
        JsonObject expected = buildExpectedJsonObject("addQueueItem", new JsonObject()
                .put(QUEUENAME, "my_queue_name")
                .put(BUFFER, "buffer_value"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildGetQueueItemOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetQueueItemOperation("my_queue_name", 22);
        JsonObject expected = buildExpectedJsonObject("getQueueItem", new JsonObject()
                .put(QUEUENAME, "my_queue_name")
                .put(INDEX, 22));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildReplaceQueueItemOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildReplaceQueueItemOperation("my_queue_name", 22, "buffer_value");
        JsonObject expected = buildExpectedJsonObject("replaceQueueItem", new JsonObject()
                .put(QUEUENAME, "my_queue_name")
                .put(INDEX, 22)
                .put(BUFFER, "buffer_value"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildDeleteQueueItemOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildDeleteQueueItemOperation("my_queue_name", 11);
        JsonObject expected = buildExpectedJsonObject("deleteQueueItem", new JsonObject()
                .put(QUEUENAME, "my_queue_name")
                .put(INDEX, 11));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildDeleteAllQueueItemsOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildDeleteAllQueueItemsOperation("my_queue_name");
        JsonObject expected = buildExpectedJsonObject("deleteAllQueueItems", new JsonObject()
                .put(QUEUENAME, "my_queue_name").put(UNLOCK, false));
        context.assertEquals(expected, operation);

        operation = RedisquesAPI.buildDeleteAllQueueItemsOperation("my_queue_name", false);
        expected = buildExpectedJsonObject("deleteAllQueueItems", new JsonObject()
                .put(QUEUENAME, "my_queue_name").put(UNLOCK, false));
        context.assertEquals(expected, operation);

        operation = RedisquesAPI.buildDeleteAllQueueItemsOperation("my_queue_name", true);
        expected = buildExpectedJsonObject("deleteAllQueueItems", new JsonObject()
                .put(QUEUENAME, "my_queue_name").put(UNLOCK, true));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildBulkDeleteQueuesOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildBulkDeleteQueuesOperation(new JsonArray().add("q_1").add("q_2"));
        JsonObject expected = buildExpectedJsonObject("bulkDeleteQueues", new JsonObject()
                .put(QUEUES, new JsonArray().add("q_1").add("q_2")));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildGetQueuesOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetQueuesOperation();
        context.assertEquals(buildExpectedJsonObject("getQueues"), operation);

        operation = RedisquesAPI.buildGetQueuesOperation("abc");
        JsonObject expected = buildExpectedJsonObject("getQueues", new JsonObject()
                .put(FILTER, "abc"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildGetQueuesCountOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetQueuesCountOperation();
        context.assertEquals(buildExpectedJsonObject("getQueuesCount"), operation);

        operation = RedisquesAPI.buildGetQueuesCountOperation("abc");
        JsonObject expected = buildExpectedJsonObject("getQueuesCount", new JsonObject()
                .put(FILTER, "abc"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildGetQueueItemsCountOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetQueueItemsCountOperation("my_queue_name");
        JsonObject expected = buildExpectedJsonObject("getQueueItemsCount", new JsonObject()
                .put(QUEUENAME, "my_queue_name"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildGetLockOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetLockOperation("my_queue_name");
        JsonObject expected = buildExpectedJsonObject("getLock", new JsonObject()
                .put(QUEUENAME, "my_queue_name"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildDeleteLockOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildDeleteLockOperation("my_queue_name");
        JsonObject expected = buildExpectedJsonObject("deleteLock", new JsonObject()
                .put(QUEUENAME, "my_queue_name"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildDeleteAllLocksOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildDeleteAllLocksOperation();
        context.assertEquals(buildExpectedJsonObject("deleteAllLocks"), operation);
    }

    @Test
    public void testBuildBulkDeleteLocksOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildBulkDeleteLocksOperation(new JsonArray().add("lock_1").add("lock_2").add("lock_3"));

        JsonObject expected = buildExpectedJsonObject("bulkDeleteLocks", new JsonObject()
                .put(LOCKS, new JsonArray().add("lock_1").add("lock_2").add("lock_3")));
        context.assertEquals(expected, operation);
    }


    @Test
    public void testBuildPutLockOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildPutLockOperation("my_queue_name", "request_user");
        JsonObject expected = buildExpectedJsonObject("putLock", new JsonObject()
                .put(QUEUENAME, "my_queue_name")
                .put(REQUESTED_BY, "request_user"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildBulkPutLocksOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildBulkPutLocksOperation(new JsonArray().add("lock_1").add("lock_2").add("lock_3"), "request_user");

        JsonObject expected = buildExpectedJsonObject("bulkPutLocks", new JsonObject()
                .put(LOCKS, new JsonArray().add("lock_1").add("lock_2").add("lock_3"))
                .put(REQUESTED_BY, "request_user"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildGetAllLocksOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetAllLocksOperation();
        context.assertEquals(buildExpectedJsonObject("getAllLocks"), operation);

        operation = RedisquesAPI.buildGetAllLocksOperation("abc");
        JsonObject expected = buildExpectedJsonObject("getAllLocks", new JsonObject()
                .put(FILTER, "abc"));
        context.assertEquals(expected, operation);
    }

    @Test
    public void testBuildCheckOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildCheckOperation();
        context.assertEquals(buildExpectedJsonObject("check"), operation);
    }

    @Test
    public void testBuildGetConfigurationOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildGetConfigurationOperation();
        context.assertEquals(buildExpectedJsonObject("getConfiguration"), operation);
    }

    @Test
    public void testBuildSetConfigurationOperation(TestContext context) throws Exception {
        JsonObject operation = RedisquesAPI.buildSetConfigurationOperation(new JsonObject().put(PROCESSOR_DELAY_MAX, 99));
        JsonObject expected = buildExpectedJsonObject("setConfiguration", new JsonObject()
                .put(PROCESSOR_DELAY_MAX, 99));
        context.assertEquals(expected, operation);
    }

    private JsonObject buildExpectedJsonObject(String operation){
        JsonObject expected = new JsonObject();
        expected.put("operation", operation);
        return expected;
    }

    private JsonObject buildExpectedJsonObject(String operation, JsonObject payload){
        JsonObject expected = buildExpectedJsonObject(operation);
        expected.put("payload", payload);
        return expected;
    }

    private JsonObject buildExpectedJsonObject(String operation, JsonObject payload, String message){
        JsonObject expected = buildExpectedJsonObject(operation, payload);
        expected.put("message", message);
        return expected;
    }
}
