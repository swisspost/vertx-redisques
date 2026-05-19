package org.swisspush.redisques.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.regex.Pattern;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueueConfiguration {

    /**
     * This configuration applies to queues with names matching this RegEx
     */
    private Pattern pattern;

    /**
     * after a failed de-queuing the next try is delayed by the first entry (values are seconds)
     * The 2nd failed de-queue takes the seconds array-entry for delay, the 3rd takes the 3rd, etc...
     */
    private int[] retryIntervals = new int[0];

    /**
     * EN-queuing speed can be throttled by delaying the "ok" response.
     * <p>
     * This is a simple linear factor. E.g. when set to "300" this means that
     * "success" enqueuing-reply is delayed for 300 ms (0.3 seconds= when queue is of
     * length 1000 (0.6 seconds when length is 2000 etc...)
     * <p>
     * "0" means: turn of this feature
     */
    private float enqueueDelayFactorMillis = 0f;

    /**
     * When EN-queue slowdown is used ({@link #enqueueDelayFactorMillis}) you can limit the maximum delay here.
     * E.g. when set to "1000" you still have a maximum EN-queuing rate of "one per second" - even when the queue already is very large
     * <p>
     * default "0" means: no limit
     */
    private int enqueueMaxDelayMillis = 0;

    /**
     * Maximum queue items allowed in queue, as FIFO (First-In, First-Out) limited queue.
     * default "0" means: no limit
     */
    private int maxQueueEntries = 0;

    /**
     * Maximum queue items allow to be enqueued, over limit, the newly coming will reject
     * default "0" means: no limit
     */
    private long enqueuePatrolLimit = 0l;

    public QueueConfiguration(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    /**
     * constructor use for Json deserialize
     */
    QueueConfiguration() {
    }

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

    public int getMaxQueueEntries() {
        return maxQueueEntries;
    }

    public Long getEnqueuePatrolLimit() {
        return enqueuePatrolLimit;
    }

    public JsonObject asJsonObject() {
        return JsonObject.mapFrom(this);
    }

    public static QueueConfiguration fromJsonObject(JsonObject jsonObject) {
        return jsonObject.mapTo(QueueConfiguration.class);
    }

    /**
     * set the config filter regex pattern
     *
     * @param pattern
     * @return
     */
    public QueueConfiguration withPattern(String pattern) {
        // this also checks for correct RegEx-Pattern
        this.pattern = Pattern.compile(pattern);
        return this;
    }

    /**
     * set the queue slowdown retry intervals. empty array to disable this function
     *
     * @param retryIntervals
     * @return
     */
    public QueueConfiguration withRetryIntervals(int... retryIntervals) {
        for (int retryInterval : retryIntervals) {
            if (retryInterval < 1) {
                throw new IllegalArgumentException("retryIntervals must all be >=1 (second) but is " + Arrays.toString(retryIntervals));
            }
        }
        this.retryIntervals = retryIntervals;
        return this;
    }

    /**
     * set the enqueue delay factor, set to 0 to disable this function. if set to 0, the value in enqueue max delay will ignore,
     * enqueue reply will not delay
     *
     * @param enqueueDelayFactorMillis
     * @return
     */
    public QueueConfiguration withEnqueueDelayMillisPerSize(float enqueueDelayFactorMillis) {
        if (enqueueDelayFactorMillis < 0f) {
            throw new IllegalArgumentException("enqueueDelayMillisPerSize must be >=0 but is " + enqueueDelayFactorMillis);
        }
        this.enqueueDelayFactorMillis = enqueueDelayFactorMillis;
        return this;
    }

    /**
     * set the enqueue max delay, if set to 0, will take delay value calculated from enqueueDelayFactorMillis without limit
     *
     * @param enqueueMaxDelayMillis
     * @return
     */
    public QueueConfiguration withEnqueueMaxDelayMillis(int enqueueMaxDelayMillis) {
        if (enqueueMaxDelayMillis < 0) {
            throw new IllegalArgumentException("enqueueMaxDelayMillis must be >=0 but is " + enqueueMaxDelayMillis);
        }
        this.enqueueMaxDelayMillis = enqueueMaxDelayMillis;
        return this;
    }

    /**
     * set the max queue items can retain in a queue, the new item can still enqueued, but old item will be removed if limits reached
     *
     * @param maxQueueEntries
     * @return
     */
    public QueueConfiguration withMaxQueueEntries(int maxQueueEntries) {
        if (maxQueueEntries < 0) {
            throw new IllegalArgumentException("maxQueueEntries must be >=0 but is " + maxQueueEntries);
        }
        this.maxQueueEntries = maxQueueEntries;
        return this;
    }

    /**
     * set the max queue items can enqueue in a queue, the new item will reject if limits reached
     *
     * @param enqueuePatrolLimit
     * @return
     */
    public QueueConfiguration withEnqueuePatrolLimit(int enqueuePatrolLimit) {
        if (enqueuePatrolLimit < 0) {
            throw new IllegalArgumentException("enqueuePatrolLimit must be >=0 but is " + enqueuePatrolLimit);
        }
        this.enqueuePatrolLimit = enqueuePatrolLimit;
        return this;
    }
}
