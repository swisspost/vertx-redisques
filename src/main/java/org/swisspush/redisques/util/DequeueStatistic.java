package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;

import javax.annotation.Nullable;

public class DequeueStatistic {

    public static final String KEY_LAST_DEQUEUE_ATTEMPT_TS = "lastDequeueAttemptTimestamp";
    public static final String KEY_LAST_DEQUEUE_SUCCESS_TS = "lastDequeueSuccessTimestamp";
    public static final String KEY_NEXT_DEQUEUE_DUE_TS = "nextDequeueDueTimestamp";
    public static final String KEY_LAST_UPDATED_TS = "lastUpdatedTimestamp";
    public static final String KEY_FAILED_REASON = "failedReason";
    public static final String KEY_MARK_FOR_REMOVAL = "markForRemoval";

    private Long lastDequeueAttemptTimestamp = null;
    private Long lastDequeueSuccessTimestamp = null;
    private Long nextDequeueDueTimestamp = null;
    private Long lastUpdatedTimestamp = null;
    private String failedReason = null;
    private boolean markForRemoval = false;

    public DequeueStatistic() {
        // default
    }

    public DequeueStatistic(
            Long lastDequeueAttemptTimestamp,
            Long lastDequeueSuccessTimestamp,
            Long nextDequeueDueTimestamp,
            Long lastUpdatedTimestamp,
            String failedReason,
            boolean markForRemoval
    ) {
        this.lastDequeueAttemptTimestamp = lastDequeueAttemptTimestamp;
        this.lastDequeueSuccessTimestamp = lastDequeueSuccessTimestamp;
        this.nextDequeueDueTimestamp = nextDequeueDueTimestamp;
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
        this.failedReason = failedReason;
        this.markForRemoval = markForRemoval;
    }

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


    private static void putIfNotNull(JsonObject json, String key, Object value) {
        if (value != null) {
            json.put(key, value);
        }
    }
    /**
     * Convert this object to Vert.x JsonObject
     */
    public JsonObject asJson() {
        JsonObject json = new JsonObject();
        putIfNotNull(json, KEY_LAST_DEQUEUE_ATTEMPT_TS, lastDequeueAttemptTimestamp);
        putIfNotNull(json, KEY_LAST_DEQUEUE_SUCCESS_TS, lastDequeueSuccessTimestamp);
        putIfNotNull(json, KEY_NEXT_DEQUEUE_DUE_TS, nextDequeueDueTimestamp);
        putIfNotNull(json, KEY_LAST_UPDATED_TS, lastUpdatedTimestamp);
        putIfNotNull(json, KEY_FAILED_REASON, failedReason);

        // always include boolean
        json.put(KEY_MARK_FOR_REMOVAL, markForRemoval);

        return json;
    }

    /**
     * Reconstruct object from JsonObject, safe for Redis / Vert.x storage.
     * @param json
     * @return a DequeueStatistic, if param is null, will return null
     */
    @Nullable
    public static DequeueStatistic fromJson(@Nullable JsonObject json) {
        if (json == null)
        {
            return null;
        }
        Long lastAtt = json.getLong(KEY_LAST_DEQUEUE_ATTEMPT_TS);
        Long lastSuc = json.getLong(KEY_LAST_DEQUEUE_SUCCESS_TS);
        Long nextDue = json.getLong(KEY_NEXT_DEQUEUE_DUE_TS);
        Long lastUpd = json.getLong(KEY_LAST_UPDATED_TS);
        String reason = json.getString(KEY_FAILED_REASON);

        Boolean flag = json.getBoolean(KEY_MARK_FOR_REMOVAL, false);

        return new DequeueStatistic(
                lastAtt,
                lastSuc,
                nextDue,
                lastUpd,
                reason,
                flag
        );
    }
}
