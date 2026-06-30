package org.swisspush.redisques.util;

import io.vertx.core.buffer.Buffer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueueSizeInfoMapCodecTest {
    private final QueueSizeInfoMapCodec codec = new QueueSizeInfoMapCodec();

    @Test
    public void shouldEncodeAndDecodeQueueSizeInfoMap() {
        QueueSizeInfoMap infoMap = new QueueSizeInfoMap();
        Map<String, QueueSizeInfoEntry> consumer1Queues = new HashMap<>();
        consumer1Queues.put("queue-a", new QueueSizeInfoEntry(10L, 1000L));
        consumer1Queues.put("queue-b", new QueueSizeInfoEntry(20L, 2000L));

        Map<String, QueueSizeInfoEntry> consumer2Queues = new HashMap<>();
        consumer2Queues.put("queue-c", new QueueSizeInfoEntry(30L, 3000L));

        infoMap.put("consumer-1", consumer1Queues);
        infoMap.put("consumer-2", consumer2Queues);

        Buffer buffer = Buffer.buffer();
        codec.encodeToWire(buffer, infoMap);

        QueueSizeInfoMap decoded = codec.decodeFromWire(0, buffer);

        assertNotNull(decoded);
        assertEquals(2, decoded.getValue().size());

        QueueSizeInfoEntry queueA =
                decoded.getValue().get("consumer-1").get("queue-a");

        assertEquals(10L, queueA.getQueueItemSizeCounter());
        assertEquals(1000L, queueA.getLastRegisterRefreshedMillis());

        QueueSizeInfoEntry queueB =
                decoded.getValue().get("consumer-1").get("queue-b");

        assertEquals(20L, queueB.getQueueItemSizeCounter());
        assertEquals(2000L, queueB.getLastRegisterRefreshedMillis());

        QueueSizeInfoEntry queueC =
                decoded.getValue().get("consumer-2").get("queue-c");

        assertEquals(30L, queueC.getQueueItemSizeCounter());
        assertEquals(3000L, queueC.getLastRegisterRefreshedMillis());
    }

    @Test
    public void shouldEncodeAndDecodeEmptyMap() {
        QueueSizeInfoMap body = new QueueSizeInfoMap();

        Buffer buffer = Buffer.buffer();
        codec.encodeToWire(buffer, body);

        QueueSizeInfoMap decoded = codec.decodeFromWire(0, buffer);

        assertNotNull(decoded);
        assertNotNull(decoded.getValue());
        assertTrue(decoded.getValue().isEmpty());
    }
}
