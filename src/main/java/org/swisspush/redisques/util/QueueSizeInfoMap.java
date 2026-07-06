package org.swisspush.redisques.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueueSizeInfoMap {

    private final Map<String, Map<String, QueueSizeInfoEntry>> value = new ConcurrentHashMap<>();

    public QueueSizeInfoMap() {
    }

    public Map<String, Map<String, QueueSizeInfoEntry>> getValue() {
        return value;
    }

    public void putAll(QueueSizeInfoMap localQueueSize) {
        value.putAll(localQueueSize.value);
    }

    public void put(String verticleUid, Map<String, QueueSizeInfoEntry> queueSizeMapWithTimeInfo) {
        value.put(verticleUid, queueSizeMapWithTimeInfo);
    }
}