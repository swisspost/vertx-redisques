package org.swisspush.redisques.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class QueueSizeInfoEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long lastRegisterRefreshedMillis;
    private final long queueItemSize;

    @JsonCreator
    public QueueSizeInfoEntry(
            @JsonProperty("queueItemSizeCounter") long size,
            @JsonProperty("lastRegisterRefreshedMillis") long timestampMillis) {
        this.queueItemSize = size;
        this.lastRegisterRefreshedMillis = timestampMillis;
    }

    public long getLastRegisterRefreshedMillis() {
        return lastRegisterRefreshedMillis;
    }

    public long getQueueItemSizeCounter() {
        return queueItemSize;
    }
}