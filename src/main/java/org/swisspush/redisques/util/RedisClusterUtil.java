package org.swisspush.redisques.util;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * order all keys by it slot hash
     * @param keys keys will order
     * @return
     */
    public static List<String> orderKeysBySlot(List<String> keys) {
        Map<Integer, List<String>> slotMap = groupKeysBySlot(keys);

        List<String> ordered = new ArrayList<>(keys.size());

        for (List<String> slotKeys : slotMap.values()) {
            ordered.addAll(slotKeys);
        }

        return ordered;
    }

    /**
     * group all keys by it slot hash
     * @param keys keys will group
     * @return
     */
    public static Map<Integer, List<String>> groupKeysBySlot(List<String> keys) {
        Map<Integer, List<String>> slotKeys = new HashMap<>();
        for (String key : keys) {
            int slot = redisSlot(key);
            slotKeys.computeIfAbsent(slot, s -> new ArrayList<>()).add(key);
        }
        return slotKeys;
    }

    /**
     * group my queue list by key's slot hash
     *
     * @param queues data will group
     * @return
     */
    public static <T> Map<String, T> groupMapBySlot(
            Map<String, T> queues) {

        Map<Integer, LinkedHashMap<String, T>> slotGroups = new HashMap<>();

        for (Map.Entry<String, T> entry : queues.entrySet()) {
            int slot = redisSlot(entry.getKey());
            slotGroups
                    .computeIfAbsent(slot, s -> new LinkedHashMap<>())
                    .put(entry.getKey(), entry.getValue());
        }

        List<Integer> sortedSlots = new ArrayList<>(slotGroups.keySet());
        Collections.sort(sortedSlots);

        Map<String, T> result = new LinkedHashMap<>();
        for (Integer slot : sortedSlots) {
            result.putAll(slotGroups.get(slot));
        }
        return result;
    }

    /**
     * calculate the slot hash of a key
     * @param key will use to calculate
     * @return
     */
    public static int redisSlot(String key) {
        int start = key.indexOf('{');
        if (start != -1) {
            int end = key.indexOf('}', start + 1);
            if (end != -1 && end > start + 1) {
                return crc16(key.getBytes(), start + 1, end) % 16384;
            }
        }
        return crc16(key.getBytes(), 0, key.getBytes().length) % 16384;
    }

    static int crc16(byte[] bytes, int start, int end) {
        int crc = 0;
        for (int i = start; i < end; i++) {
            crc = ((crc << 8) ^ LOOKUP_TABLE[((crc >> 8) ^ (bytes[i] & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }
}
