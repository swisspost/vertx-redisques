package org.swisspush.redisques.util;

import java.nio.charset.StandardCharsets;

public class RedisClusterUtil {
    private static final int[] LOOKUP_TABLE = new int[256];
    static {
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                crc = ((crc << 1) ^ (((crc & 0x8000) != 0) ? 0x1021 : 0));
            }
            LOOKUP_TABLE[i] = crc & 0xFFFF;
        }
    }

    public static int redisSlot(String key) {
        String hashtag = getHashTag(key);
        byte[] bytes = hashtag.getBytes(StandardCharsets.UTF_8);
        return crc16(bytes) % 16384;
    }

    static String getHashTag(String key) {
        int start = key.indexOf('{');
        if (start != -1) {
            int end = key.indexOf('}', start + 1);
            if (end != -1 && end != start + 1) {
                return key.substring(start + 1, end);
            }
        }
        return key;
    }

    static int crc16(byte[] bytes) {
        int crc = 0;
        for (byte b : bytes) {
            crc = ((crc << 8) ^ LOOKUP_TABLE[((crc >> 8) ^ (b & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }
}
