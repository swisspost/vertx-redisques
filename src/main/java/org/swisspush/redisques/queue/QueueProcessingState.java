package org.swisspush.redisques.queue;

import org.swisspush.redisques.QueueState;

public class QueueProcessingState {
    private QueueState state;
    private long lastConsumedTimestampMillis;
    private long lastRegisterRefreshedMillis;
    private volatile long queueItemSize;
    private RebalanceClaimState rebalanceClaimState = RebalanceClaimState.NONE;
    private String rebalancePreviousOwner;
    private long rebalancePendingSinceMillis;
    private boolean rebalanceRecoveryInFlight;

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

    synchronized boolean isRebalancePending() {
        return rebalanceClaimState != RebalanceClaimState.NONE;
    }

    synchronized void setRebalancePending(String previousOwner) {
        rebalancePreviousOwner = previousOwner;
        rebalanceClaimState = RebalanceClaimState.PENDING;
        rebalancePendingSinceMillis = System.currentTimeMillis();
        rebalanceRecoveryInFlight = false;
    }

    /**
     * Reserves the right to reconcile a claim that has been pending for at least the given time.
     *
     * @return the previous owner to ask for confirmation, or null if there is nothing to recover (yet)
     */
    synchronized String beginRebalanceRecovery(long minPendingMillis) {
        if (rebalanceClaimState != RebalanceClaimState.PENDING || rebalanceRecoveryInFlight
                || System.currentTimeMillis() - rebalancePendingSinceMillis < minPendingMillis) {
            return null;
        }
        rebalanceRecoveryInFlight = true;
        return rebalancePreviousOwner;
    }

    /** Ends a recovery attempt; a still pending claim waits a full period before the next attempt. */
    synchronized void finishRebalanceRecovery() {
        rebalanceRecoveryInFlight = false;
        rebalancePendingSinceMillis = System.currentTimeMillis();
    }

    synchronized String getRebalancePreviousOwner() {
        return rebalanceClaimState == RebalanceClaimState.PENDING ? rebalancePreviousOwner : null;
    }

    synchronized boolean activateRebalanceClaim() {
        if (rebalanceClaimState != RebalanceClaimState.PENDING) {
            return false;
        }
        rebalanceClaimState = RebalanceClaimState.NONE;
        rebalancePreviousOwner = null;
        return true;
    }

    synchronized boolean beginRebalanceAbandon() {
        if (rebalanceClaimState != RebalanceClaimState.PENDING) {
            return false;
        }
        rebalanceClaimState = RebalanceClaimState.ABANDONING;
        return true;
    }

    synchronized void finishRebalanceAbandon(boolean restored) {
        rebalanceClaimState = restored ? RebalanceClaimState.NONE : RebalanceClaimState.PENDING;
        if (restored) {
            rebalancePreviousOwner = null;
        }
    }

    private enum RebalanceClaimState {
        NONE,
        PENDING,
        ABANDONING
    }
}