package org.swisspush.redisques.util;

import java.io.Serializable;

public class QueueSizeInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long lastRegisterRefreshedMillis;
    private final long queueItemSize;

    public QueueSizeInfo(long size, long timestampMillis) {
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