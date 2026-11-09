package org.swisspush.redisques.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;

/**
 * Counters for the queue rebalancing handoff. Every method is a no-op when no
 * {@link MeterRegistry} is available, so callers never need to check for it.
 */
public class RebalanceMetrics {
    public static final String RESULT_EXECUTED = "executed";

    private final MeterRegistry meterRegistry;
    private final String metricsIdentifier;

    public RebalanceMetrics(MeterRegistry meterRegistry, String metricsIdentifier) {
        this.meterRegistry = meterRegistry;
        this.metricsIdentifier = metricsIdentifier;
    }

    /** Outcome of one move handled by a rebalance request: {@code executed} or a skip reason such as {@code release-failed}. */
    public void moveResult(String result) {
        increment(MetricMeter.REBALANCE_MOVE, result);
    }

    /** Outcome of a target-side recovery of a stale pending claim: {@code activated}, {@code abandoned} or {@code failed}. */
    public void claimRecoveryResult(String result) {
        increment(MetricMeter.REBALANCE_CLAIM_RECOVERY, result);
    }

    /** This node gave up a queue to another node (the "removed" side of a move). */
    public void queueReleased() {
        increment(MetricMeter.REBALANCE_QUEUE_RELEASED, null);
    }

    /** This node started consuming a queue it received from another node (the "added" side of a move). */
    public void queueActivated() {
        increment(MetricMeter.REBALANCE_QUEUE_ACTIVATED, null);
    }

    /** This node rolled back a claim and handed the queue back to its previous owner. */
    public void queueAbandoned() {
        increment(MetricMeter.REBALANCE_QUEUE_ABANDONED, null);
    }

    private void increment(MetricMeter meter, String result) {
        if (meterRegistry == null) {
            return;
        }
        Counter.Builder builder = Counter.builder(meter.getId())
                .description(meter.getDescription())
                .tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier);
        if (result != null) {
            builder.tag(MetricTags.RESULT.getId(), result);
        }
        builder.register(meterRegistry).increment();
    }
}
