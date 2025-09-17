package org.swisspush.redisques.queue;

import org.swisspush.redisques.QueueState;

public class QueueProcessingState {
    public QueueProcessingState(QueueState state, long timestampMillis) {
        this.setState(state);
        this.setLastConsumedTimestampMillis(timestampMillis);
    }

    private QueueState state;
    private long lastConsumedTimestampMillis;

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
}