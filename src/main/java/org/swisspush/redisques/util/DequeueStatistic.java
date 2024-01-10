package org.swisspush.redisques.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DequeueStatistic {
    public Long lastDequeueAttemptTimestamp = null;
    public Long lastDequeueSuccessTimestamp = null;
    public Long nextDequeueDueTimestamp = null;

    public boolean isEmpty() {
        return lastDequeueAttemptTimestamp == null && lastDequeueSuccessTimestamp == null && nextDequeueDueTimestamp == null;
    }
}
