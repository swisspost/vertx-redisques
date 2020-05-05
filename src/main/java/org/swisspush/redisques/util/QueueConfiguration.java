package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.regex.Pattern;

public class QueueConfiguration {

    /**
     * This configuration applies to queues with names matching this RegEx
     */
    private Pattern pattern;

    /**
     * after a failed de-queuing the next try is delayed by the first entry (values are seconds)
     * The 2nd failed de-queue takes the seconds array-entry for delay, the 3rd takes the 3rd, etc...
     */
    private int[] retryIntervals;

    /**
     * EN-queuing speed can be throttled by delaying the "ok" response.
     *
     * This is a simple linear factor. E.g. when set to "300" this means that
     * "success" enqueuing-reply is delayed for 300 ms (0.3 seconds= when queue is of
     * length 1000 (0.6 seconds when length is 2000 etc...)
     *
     * "0" means: turn of this feature
     */
    private float enqueueDelayFactorMillis = 0f;

    /**
     * When EN-queue slowdown is used ({@link #enqueueDelayFactorMillis}) you can limit the maximum delay here.
     * E.g. when set to "1000" you still have a maximum EN-queuing rate of "one per second" - even when the queue already is very large
     *
     * default "0" means: no limit
     */
    private int enqueueMaxDelayMillis = 0;

    public String getPattern() {
        return pattern.pattern();
    }

    /**
     * access the precompiled Pattern
     * Explicit NOT a Getter (i.e. not named "get....") sp it's not JSON-serialized
     *
     * @return the compiled Pattern to quickly match (or not match) queue name
     */
    public Pattern compiledPattern() {
        return pattern;
    }

    public int[] getRetryIntervals() {
        return retryIntervals;
    }

    public float getEnqueueDelayFactorMillis() {
        return enqueueDelayFactorMillis;
    }

    public int getEnqueueMaxDelayMillis() {
        return enqueueMaxDelayMillis;
    }

    public JsonObject asJsonObject() {
        return JsonObject.mapFrom(this);
    }
    
    static QueueConfiguration fromJsonObject(JsonObject jsonObject) {
        return jsonObject.mapTo(QueueConfiguration.class);
    }

    public QueueConfiguration withPattern(String pattern) {
        // this also checks for correct RegEx-Pattern
        this.pattern = Pattern.compile(pattern);
        return this;
    }

    public QueueConfiguration withRetryIntervals(int... retryIntervals) {
        for (int retryInterval : retryIntervals) {
            if (retryInterval < 1) {
                throw new IllegalArgumentException("retryIntervals must all be >=1 (second) but is " + Arrays.toString(retryIntervals));
            }
        }
        this.retryIntervals = retryIntervals;
        return this;
    }

    public QueueConfiguration withEnqueueDelayMillisPerSize(float enqueueDelayFactorMillis) {
        if (enqueueDelayFactorMillis <= 0f) {
            throw new IllegalArgumentException("enqueueDelayMillisPerSize must be >0 but is " + enqueueDelayFactorMillis);
        }
        this.enqueueDelayFactorMillis = enqueueDelayFactorMillis;
        return this;
    }

    public QueueConfiguration withEnqueueMaxDelayMillis(int enqueueMaxDelayMillis) {
        if (enqueueMaxDelayMillis < 0) {
            throw new IllegalArgumentException("enqueueMaxDelayMillis must be >=0 but is " + enqueueMaxDelayMillis);
        }
        this.enqueueMaxDelayMillis = enqueueMaxDelayMillis;
        return this;
    }
}
