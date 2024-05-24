package org.swisspush.redisques.performance;

import io.vertx.core.Vertx;
import org.slf4j.Logger;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

import static java.lang.Float.NaN;
import static java.lang.System.currentTimeMillis;
import static org.slf4j.LoggerFactory.getLogger;


/**
 * Collects 1/5/15 minutes measurements of GarbageCollection activity.
 * See {@link Measurement} about collected data.
 */
public class JavaGcStats {

    private static final Logger log = getLogger(JavaGcStats.class);
    private static final Object workerInitLock = new Object();
    private static final Object measurementLock = new Object();
    private static volatile long workerHeartbeatEpochMs = 0;
    private static Worker worker;
    /** Most recent values. For their meaning see doc of {@link Measurement} fields. */
    private static float gcFrac01 = 0, gcFrac05 = 0, gcFrac15 = 0;
    private final Vertx vertx;

    public JavaGcStats(Vertx vertx){
        this.vertx = vertx;
        initObserver();
    }

    /** Fills in the values into the passed structure */
    public Measurement fillMeasurement(Measurement measurement) {
        long workerHeartbeatAgeMs = currentTimeMillis() - workerHeartbeatEpochMs;
        boolean workerMayDied = workerHeartbeatAgeMs > 4 * Worker.MEASURE_ALL_N_MILLIS;
        if (workerMayDied) {
            log.warn("Huh? No worker heartbeat since {}ms?", workerHeartbeatAgeMs);
        }
        synchronized (measurementLock) {
            measurement.gcFrac01 = gcFrac01;
            measurement.gcFrac05 = gcFrac05;
            measurement.gcFrac15 = gcFrac15;
            if (workerMayDied) {
                gcFrac01 = gcFrac05 = gcFrac15 = NaN;
            }
        }
        return measurement;
    }

    private void initObserver() {
        synchronized (workerInitLock) {
            if( worker == null ){
                worker = new Worker();
                worker.setDaemon(true);
                worker.start();
            }
        }
   }

    private static class Worker extends Thread {

        private static final int MEASURE_ALL_N_MILLIS = 15_000;
        private static final int RECOVER_PERIOD_MS = 30_000;
        private static final int BACKLOG = (60_000 / MEASURE_ALL_N_MILLIS) * 15 + 1;
        private final LongRingbuffer measuredGcTimes = new LongRingbuffer(BACKLOG);
        private final LongRingbuffer measuredEpochMs = new LongRingbuffer(BACKLOG);
        private final long tmpLongs[] = new long[BACKLOG];
        private long previousTriggerEpochMs;

        public Worker() {
            super("RedisQues-" + JavaGcStats.class.getName());
        }

        @Override public void run() {
            assert currentThread() == this : "currentThread() == this";
            log.info("Publish every {}ms, {}ms recover period and {} backlog.",
                    MEASURE_ALL_N_MILLIS, RECOVER_PERIOD_MS, BACKLOG);
            Throwable unexpectedEx = null;
            while (true) try {
                if( unexpectedEx != null ){
                    // Reset, but cool-down before risking the exception again.
                    unexpectedEx = null;
                    sleep(RECOVER_PERIOD_MS);
                }
                assert currentThread() == worker : "currentThread() == worker";
                long nowMs = currentTimeMillis();
                if (previousTriggerEpochMs == 0) previousTriggerEpochMs = nowMs;
                // Last time plus exactly one interval.
                long timeUpToNextPeriodMs = previousTriggerEpochMs + MEASURE_ALL_N_MILLIS - nowMs;
                // skip a beat if we're too far behind.
                while (timeUpToNextPeriodMs < MEASURE_ALL_N_MILLIS / 64) timeUpToNextPeriodMs += MEASURE_ALL_N_MILLIS;
                // make state usable for next round.
                while (previousTriggerEpochMs < nowMs) previousTriggerEpochMs += MEASURE_ALL_N_MILLIS;
                log.debug("timeUpToNextPeriodMs is {}", timeUpToNextPeriodMs);
                sleep(timeUpToNextPeriodMs);
                // make a beep so consumers can notice if we died.
                workerHeartbeatEpochMs = currentTimeMillis();
                // Go do the actual GC measuring.
                fetchGcTime();
                updateStats();
            } catch (Throwable ex) {
                if (ex instanceof InterruptedException) {
                    currentThread().interrupt();
                    throw new UnsupportedOperationException("not impl", ex);
                }
                unexpectedEx = ex;
                log.error("Unexpected error while collecting GC stats", ex);
            }
        }

        private void fetchGcTime() {
            List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
            long totalGcTimeSinceJvmStartMs = 0;
            for (GarbageCollectorMXBean gcBean : garbageCollectorMXBeans) {
                long gcTimeMs = gcBean.getCollectionTime();
                if( gcTimeMs == -1 ) continue;
                assert gcTimeMs >= 0;
                totalGcTimeSinceJvmStartMs += gcTimeMs;
            }
            measuredEpochMs.add(currentTimeMillis());
            measuredGcTimes.add(totalGcTimeSinceJvmStartMs);
        }

        private void updateStats() {
            long gc01TimeMs, gc05TimeMs, gc15TimeMs;
            long gc01PeriodDurMs, gc05PeriodDurMs, gc15PeriodDurMs;
            int numValues;
            float gcFrac01 = NaN, gcFrac05 = NaN, gcFrac15 = NaN;
            int idx01 =  1 * 60_000 / MEASURE_ALL_N_MILLIS;
            int idx05 =  5 * 60_000 / MEASURE_ALL_N_MILLIS;
            int idx15 = 15 * 60_000 / MEASURE_ALL_N_MILLIS;
            do {
                // timestamps
                numValues = measuredEpochMs.read(tmpLongs, 0, tmpLongs.length);
                if (numValues <= 1){
                    log.debug("Not enough data yet");
                    break;
                }
                gc01PeriodDurMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < idx01) ? 0 : numValues - idx01];
                gc05PeriodDurMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < idx05) ? 0 : numValues - idx05];
                gc15PeriodDurMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < idx15) ? 0 : numValues - idx15];
                assert gc01PeriodDurMs >= 0 : gc01PeriodDurMs +" >= 0";
                assert gc05PeriodDurMs >= 0 : gc05PeriodDurMs +" >= 0";
                assert gc15PeriodDurMs >= 0 : gc15PeriodDurMs +" >= 0";
                // durations
                numValues = measuredGcTimes.read(tmpLongs, 0, tmpLongs.length);
                if (numValues <= 1){
                    log.debug("not enough data yet");
                    break;
                }
                gc01TimeMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < idx01) ? 0 : numValues - idx01];
                gc05TimeMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < idx05) ? 0 : numValues - idx05];
                gc15TimeMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < idx15) ? 0 : numValues - idx15];
                assert gc01TimeMs >= 0 : gc01TimeMs +" >= 0";
                assert gc05TimeMs >= 0 : gc05TimeMs +" >= 0";
                assert gc15TimeMs >= 0 : gc15TimeMs +" >= 0";
                // fractions of GC
                gcFrac01 = (gc01PeriodDurMs == 0) ? 0 : (float)gc01TimeMs / gc01PeriodDurMs;
                gcFrac05 = (gc05PeriodDurMs == 0) ? 0 : (float)gc05TimeMs / gc05PeriodDurMs;
                gcFrac15 = (gc15PeriodDurMs == 0) ? 0 : (float)gc15TimeMs / gc15PeriodDurMs;
                assert gcFrac01 >= 0 && gcFrac01 <= 1 : "0 <= "+ gcFrac01 +" <= 1";
                assert gcFrac05 >= 0 && gcFrac05 <= 1 : "0 <= "+ gcFrac05 +" <= 1";
                assert gcFrac15 >= 0 && gcFrac15 <= 1 : "0 <= "+ gcFrac15 +" <= 1";
            } while (false);
            synchronized (measurementLock) {
                JavaGcStats.gcFrac01 = gcFrac01;
                JavaGcStats.gcFrac05 = gcFrac05;
                JavaGcStats.gcFrac15 = gcFrac15;
            }
        }

    }


    public static class Measurement {

        /**
         * <p>Fraction of how much time gets consumed By garbage collection
         * during the last 1, 5 and 15 minutes.</p>
         *
         * <p>WARN: Be prepared for NaN! Those fields may be set to NaN if
         * requested information is not (or not yet) available.</p>
         *
         * <p>Example 1: If gcFrac05 is 0.25, this means that one fourth of the
         * execution time got into garbage collection.</p>
         *
         * <p>Example 2: If gcFrac15 is 0.03, this means that 3 percent of
         * execution time got into garbage collection.</p>
         */
        public float gcFrac01 = NaN, gcFrac05 = NaN, gcFrac15 = NaN;

    }


}
