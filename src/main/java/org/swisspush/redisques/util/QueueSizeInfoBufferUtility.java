package org.swisspush.redisques.util;

import io.vertx.core.buffer.Buffer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for serializing and deserializing
 * {@code Map<String, Map<String, QueueSizeInfo>>} instances to and from
 * a Vert.x {@link Buffer}.
 * <p>
 * This helper is intended for sending queue size information directly over
 * the Vert.x EventBus using the built-in {@link Buffer} codec, avoiding the
 * need for a custom {@code MessageCodec}.
 */
public final class QueueSizeInfoBufferUtility {

    private QueueSizeInfoBufferUtility() {
    }

    /**
     * Encodes a nested queue size map into a Vert.x {@link Buffer}.
     * <p>
     * The resulting buffer can be sent directly over the Vert.x EventBus
     * without requiring a custom {@code MessageCodec}.
     *
     * @param map the queue size map to encode
     * @return a buffer containing the serialized map
     */
    public static Buffer encode(Map<String, Map<String, QueueSizeInfo>> map) {
        Buffer buffer = Buffer.buffer();

        buffer.appendInt(map.size());

        for (Map.Entry<String, Map<String, QueueSizeInfo>> outer : map.entrySet()) {
            appendString(buffer, outer.getKey());

            Map<String, QueueSizeInfo> inner = outer.getValue();
            buffer.appendInt(inner.size());

            for (Map.Entry<String, QueueSizeInfo> entry : inner.entrySet()) {
                appendString(buffer, entry.getKey());

                QueueSizeInfo info = entry.getValue();
                buffer.appendLong(info.getQueueItemSizeCounter());
                buffer.appendLong(info.getLastRegisterRefreshedMillis());
            }
        }

        return buffer;
    }

    /**
     * Decodes a nested queue size map from a Vert.x {@link Buffer}.
     * <p>
     * The buffer must have been produced by {@link #encode(Map)}.
     *
     * @param buffer the buffer to decode
     * @return the reconstructed queue size map
     */
    public static Map<String, Map<String, QueueSizeInfo>> decode(Buffer buffer) {
        int pos = 0;

        int outerSize = buffer.getInt(pos);
        pos += Integer.BYTES;

        Map<String, Map<String, QueueSizeInfo>> result = new HashMap<>();

        for (int i = 0; i < outerSize; i++) {

            String outerKey = readString(buffer, pos);
            pos += stringSize(buffer, pos);

            int innerSize = buffer.getInt(pos);
            pos += Integer.BYTES;

            Map<String, QueueSizeInfo> inner = new HashMap<>();

            for (int j = 0; j < innerSize; j++) {

                String innerKey = readString(buffer, pos);
                pos += stringSize(buffer, pos);

                long size = buffer.getLong(pos);
                pos += Long.BYTES;

                long timestamp = buffer.getLong(pos);
                pos += Long.BYTES;

                inner.put(innerKey, new QueueSizeInfo(size, timestamp));
            }

            result.put(outerKey, inner);
        }

        return result;
    }

    private static void appendString(Buffer buffer, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buffer.appendInt(bytes.length);
        buffer.appendBytes(bytes);
    }

    private static String readString(Buffer buffer, int pos) {
        int len = buffer.getInt(pos);
        return buffer.getString(pos + Integer.BYTES, pos + Integer.BYTES + len);
    }

    private static int stringSize(Buffer buffer, int pos) {
        return Integer.BYTES + buffer.getInt(pos);
    }
}