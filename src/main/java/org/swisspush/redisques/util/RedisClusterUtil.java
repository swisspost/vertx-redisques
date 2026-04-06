package org.swisspush.redisques.util;


import io.vertx.redis.client.impl.ZModem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class that calculate redis key hash, and group key by hash
 */
public class RedisClusterUtil {

    /**
     * order all keys by it slot hash
     *
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
     *
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
     * calculate the slot hash of a key by Vertx Redis Client it owen function
     *
     * @param key will use to calculate
     * @return
     */
    public static int redisSlot(String key) {
        // simple re-use the ZModem in Vertx Redis Client
        return ZModem.generate(key);
    }
}
