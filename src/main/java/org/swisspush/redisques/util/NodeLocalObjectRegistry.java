package org.swisspush.redisques.util;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A Thread safe Object cache uses for Verticles safe singleton
 */
class NodeLocalObjectRegistry {
    private static final ConcurrentHashMap<String, Object> INSTANCES = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public static <T> T get(String key) {
        return (T) INSTANCES.get(key);
    }

    public static <T> void put(String key, T value) {
        INSTANCES.put(key, value);
    }

    @VisibleForTesting
    static <T> void reset(Class<T> clazz) {
        INSTANCES.entrySet().removeIf(e -> clazz.isInstance(e.getValue()));
    }
}
