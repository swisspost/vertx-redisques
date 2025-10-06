package org.swisspush.redisques.util;

import java.io.Serializable;

public class DequeueStatistic implements Serializable {
    private Long lastDequeueAttemptTimestamp = null;
    private Long lastDequeueSuccessTimestamp = null;
    private Long nextDequeueDueTimestamp = null;
    private Long lastUpdatedTimestamp = null;
    private String failedReason = null;
    private boolean markForRemoval = false;

    private void updateLastUpdatedTimestamp() {
        this.lastUpdatedTimestamp = System.currentTimeMillis();
    }

    public boolean isEmpty() {
        return lastDequeueAttemptTimestamp == null && lastDequeueSuccessTimestamp == null && nextDequeueDueTimestamp == null;
    }

    public void setLastDequeueAttemptTimestamp(Long timestamp) {
        this.lastDequeueAttemptTimestamp = timestamp;
        updateLastUpdatedTimestamp();
    }

    public Long getLastDequeueAttemptTimestamp() {
        return this.lastDequeueAttemptTimestamp;
    }

    public void setLastDequeueSuccessTimestamp(Long timestamp) {
        this.lastDequeueSuccessTimestamp = timestamp;
        this.nextDequeueDueTimestamp = null;
        this.failedReason = null;
        updateLastUpdatedTimestamp();
    }

    public Long getLastDequeueSuccessTimestamp() {
        return this.lastDequeueSuccessTimestamp;
    }

    public void setNextDequeueDueTimestamp(Long timestamp, String reason) {
        this.nextDequeueDueTimestamp = timestamp;
        this.failedReason = reason;
        updateLastUpdatedTimestamp();
    }

    public Long getNextDequeueDueTimestamp() {
        return this.nextDequeueDueTimestamp;
    }

    public Long getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public String getFailedReason() {
        return failedReason;
    }

    public void setMarkedForRemoval() {
        this.markForRemoval = true;
        updateLastUpdatedTimestamp();
    }

    public boolean isMarkedForRemoval() {
        return this.markForRemoval;
    }
}
