package org.swisspush.redisques;

// State of each queue. Consuming means there is a message being processed.
public enum QueueState {
    READY, CONSUMING
}
