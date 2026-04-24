package org.swisspush.redisques.queue;

import org.swisspush.redisques.QueueState;

public class QueueProcessingState {
    private QueueState state;
    private long lastConsumedTimestampMillis;
    private long lastRegisterRefreshedMillis;
    private volatile long queueItemSize;

    public QueueProcessingState(QueueState state, long timestampMillis) {
        this.setState(state);
        this.setLastConsumedTimestampMillis(timestampMillis);
        this.setLastRegisterRefreshedMillis(timestampMillis);
    }

    public QueueState getState() {
        return state;
    }

    public void setState(QueueState state) {
        this.state = state;
    }

    public long getLastConsumedTimestampMillis() {
        return lastConsumedTimestampMillis;
    }

    public void setLastConsumedTimestampMillis(long lastConsumedTimestampMillis) {
        this.lastConsumedTimestampMillis = lastConsumedTimestampMillis;
    }

    public long getLastRegisterRefreshedMillis() {
        return lastRegisterRefreshedMillis;
    }

    public void setLastRegisterRefreshedMillis(long lastRegisterRefreshedMillis) {
        this.lastRegisterRefreshedMillis = lastRegisterRefreshedMillis;
    }

    public long getQueueItemSizeCounter() {
        return queueItemSize;
    }

    public void setQueueItemSize(long value) {
        queueItemSize = value;
    }
}