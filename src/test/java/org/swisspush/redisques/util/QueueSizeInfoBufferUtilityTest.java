package org.swisspush.redisques.util;

import io.vertx.core.buffer.Buffer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueueSizeInfoBufferUtilityTest {
    @Test
    public void encodeAndDecodeShouldPreserveSingleEntry() {
        Map<String, Map<String, QueueSizeInfo>> source = new HashMap<>();

        Map<String, QueueSizeInfo> inner = new HashMap<>();
        inner.put("queue-1", new QueueSizeInfo(10L, 1000L));
        source.put("consumer-1", inner);

        Buffer buffer = QueueSizeInfoBufferUtility.encode(source);
        Map<String, Map<String, QueueSizeInfo>> decoded = QueueSizeInfoBufferUtility.decode(buffer);

        assertEquals(1, decoded.size());
        assertTrue(decoded.containsKey("consumer-1"));
        assertTrue(decoded.get("consumer-1").containsKey("queue-1"));

        QueueSizeInfo decodedInfo = decoded.get("consumer-1").get("queue-1");
        assertEquals(10L, decodedInfo.getQueueItemSizeCounter());
        assertEquals(1000L, decodedInfo.getLastRegisterRefreshedMillis());
    }

    @Test
    public void encodeAndDecodeShouldPreserveMultipleEntries() {
        Map<String, Map<String, QueueSizeInfo>> source = new HashMap<>();

        Map<String, QueueSizeInfo> inner1 = new HashMap<>();
        inner1.put("queue-1", new QueueSizeInfo(10L, 1000L));
        inner1.put("queue-2", new QueueSizeInfo(20L, 2000L));

        Map<String, QueueSizeInfo> inner2 = new HashMap<>();
        inner2.put("queue-3", new QueueSizeInfo(30L, 3000L));
        inner2.put("queue-4", new QueueSizeInfo(40L, 4000L));

        source.put("consumer-1", inner1);
        source.put("consumer-2", inner2);

        Buffer buffer = QueueSizeInfoBufferUtility.encode(source);
        Map<String, Map<String, QueueSizeInfo>> decoded = QueueSizeInfoBufferUtility.decode(buffer);

        assertQueueSizeInfo(decoded, "consumer-1", "queue-1", 10L, 1000L);
        assertQueueSizeInfo(decoded, "consumer-1", "queue-2", 20L, 2000L);
        assertQueueSizeInfo(decoded, "consumer-2", "queue-3", 30L, 3000L);
        assertQueueSizeInfo(decoded, "consumer-2", "queue-4", 40L, 4000L);
    }

    @Test
    public void encodeAndDecodeShouldPreserveEmptyOuterMap() {
        Map<String, Map<String, QueueSizeInfo>> source = new HashMap<>();

        Buffer buffer = QueueSizeInfoBufferUtility.encode(source);
        Map<String, Map<String, QueueSizeInfo>> decoded = QueueSizeInfoBufferUtility.decode(buffer);

        assertNotNull(decoded);
        assertTrue(decoded.isEmpty());
    }

    @Test
    public void encodeAndDecodeShouldPreserveEmptyInnerMap() {
        Map<String, Map<String, QueueSizeInfo>> source = new HashMap<>();
        source.put("consumer-1", new HashMap<>());

        Buffer buffer = QueueSizeInfoBufferUtility.encode(source);
        Map<String, Map<String, QueueSizeInfo>> decoded = QueueSizeInfoBufferUtility.decode(buffer);

        assertNotNull(decoded);
        assertTrue(decoded.containsKey("consumer-1"));
        assertNotNull(decoded.get("consumer-1"));
        assertTrue(decoded.get("consumer-1").isEmpty());
    }

    @Test
    public void encodeAndDecodeShouldPreserveLargeLongValues() {
        Map<String, Map<String, QueueSizeInfo>> source = new HashMap<>();

        Map<String, QueueSizeInfo> inner = new HashMap<>();
        inner.put("queue-max", new QueueSizeInfo(Long.MAX_VALUE, Long.MAX_VALUE - 1));
        source.put("consumer-max", inner);

        Buffer buffer = QueueSizeInfoBufferUtility.encode(source);
        Map<String, Map<String, QueueSizeInfo>> decoded = QueueSizeInfoBufferUtility.decode(buffer);

        assertQueueSizeInfo(
                decoded,
                "consumer-max",
                "queue-max",
                Long.MAX_VALUE,
                Long.MAX_VALUE - 1
        );
    }

    private static void assertQueueSizeInfo(
            Map<String, Map<String, QueueSizeInfo>> decoded,
            String outerKey,
            String innerKey,
            long expectedSize,
            long expectedTimestamp
    ) {
        assertTrue(decoded.containsKey(outerKey));
        assertTrue(decoded.get(outerKey).containsKey(innerKey));

        QueueSizeInfo info = decoded.get(outerKey).get(innerKey);
        assertNotNull(info);
        assertEquals(expectedSize, info.getQueueItemSizeCounter());
        assertEquals(expectedTimestamp, info.getLastRegisterRefreshedMillis());
    }
}
