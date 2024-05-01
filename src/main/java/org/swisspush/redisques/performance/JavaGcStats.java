package org.swisspush.redisques.performance;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

import static java.lang.Float.NaN;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;


public class JavaGcStats {

    private static final Logger log = getLogger(JavaGcStats.class);
    private static final Object workerInitLock = new Object();
    private static final Object measurementLock = new Object();
    private static volatile long workerHeartbeatEpochMs = currentTimeMillis();
    private static Worker worker;
    /** <p>Most recent values. For their meaning see doc of {@link Measurement}
     *  fields.</p> */
    private static float gcFrac01 = 0, gcFrac05 = 0, gcFrac15 = 0;

    public JavaGcStats(Vertx vertx){
        vertx.<Void>executeBlocking(this::initObserver, false, fut -> {
            if( fut.failed() ) throw new RuntimeException("", fut.cause());
        });
    }

    /** Fills in the values into the passed structure */
    public void fillMeasurement(Measurement measurement) {
        long workerHeartbeatAgeMs = currentTimeMillis() - workerHeartbeatEpochMs;
        if ( workerHeartbeatAgeMs > 60_000) {
            log.warn("Huh? No worker heartbeat since {}ms?", workerHeartbeatAgeMs);
        }
        synchronized (measurementLock) {
            measurement.gcFrac01 = gcFrac01;
            measurement.gcFrac05 = gcFrac05;
            measurement.gcFrac15 = gcFrac15;
        }
    }

    private void initObserver(Promise<Void> onDone) {
        try {
            assert !currentThread().getName().toUpperCase().contains("EVENTLOOP");
            synchronized (workerInitLock) {
                if( worker == null ){
                    worker = new Worker();
                    worker.setDaemon(true);
                    worker.start();
                }
            }
            onDone.complete();
        } catch (Throwable ex) {
            onDone.fail(ex);
        }
   }

    private /*TODO rm static*/static class Worker extends Thread {

        private static final int BACKLOG = 16;
        private final LongRingbuffer measuredGcTimes = new LongRingbuffer(BACKLOG);
        private final LongRingbuffer measuredEpochMs = new LongRingbuffer(BACKLOG);
        private final long tmpLongs[] = new long[BACKLOG];

        @Override public void run() {
            assert currentThread() == this;
            Throwable unexpectedEx = null;
            while (true) try {
                workerHeartbeatEpochMs = currentTimeMillis();
                if( unexpectedEx != null ){
                    // Reset, but cool-down before risking the exception again.
                    unexpectedEx = null;
                    sleep(5000);
                }
                assert currentThread() == worker;
                step();
            } catch (Throwable ex) {
                if (ex instanceof InterruptedException) currentThread().interrupt();
                unexpectedEx = ex;
                log.error("Unexpected error while collecting GC stats", ex);
            }
        }

        private void step() throws InterruptedException {
            sleep(5000);
            fetchGcTime();
            updateStats();
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
            // timestamps
            numValues = measuredEpochMs.read(tmpLongs, 0, tmpLongs.length);
            if (numValues <= 0) return;
            gc01PeriodDurMs = tmpLongs[numValues - 1] - tmpLongs[(numValues <  2) ? 0 : numValues -  2];
            gc05PeriodDurMs = tmpLongs[numValues - 1] - tmpLongs[(numValues <  6) ? 0 : numValues -  6];
            gc15PeriodDurMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < 16) ? 0 : numValues - 16];
            assert gc01PeriodDurMs >= 0 : gc01PeriodDurMs +" >= 0";
            assert gc05PeriodDurMs >= 0 : gc05PeriodDurMs +" >= 0";
            assert gc15PeriodDurMs >= 0 : gc15PeriodDurMs +" >= 0";
            // durations
            numValues = measuredGcTimes.read(tmpLongs, 0, tmpLongs.length);
            if (numValues <= 0) return;
            gc01TimeMs = tmpLongs[numValues - 1] - tmpLongs[(numValues <  2) ? 0 : numValues -  2];
            gc05TimeMs = tmpLongs[numValues - 1] - tmpLongs[(numValues <  6) ? 0 : numValues -  6];
            gc15TimeMs = tmpLongs[numValues - 1] - tmpLongs[(numValues < 16) ? 0 : numValues - 16];
            assert gc01TimeMs >= 0 : gc01TimeMs +" >= 0";
            assert gc05TimeMs >= 0 : gc05TimeMs +" >= 0";
            assert gc15TimeMs >= 0 : gc15TimeMs +" >= 0";
            //log.debug("got {} gcTime     : gc01 {}   gc05 {}   gc15 {}", numValues, gc01TimeMs, gc05TimeMs, gc15TimeMs);
            //log.debug("got {} periodDurMs: gc01 {}   gc05 {}   gc15 {}", numValues, gc01PeriodDurMs, gc05PeriodDurMs, gc15PeriodDurMs);
            float gcFrac01, gcFrac05, gcFrac15;
            gcFrac01 = (gc01PeriodDurMs == 0) ? 0 : (float)gc01TimeMs / gc01PeriodDurMs;
            gcFrac05 = (gc05PeriodDurMs == 0) ? 0 : (float)gc05TimeMs / gc05PeriodDurMs;
            gcFrac15 = (gc15PeriodDurMs == 0) ? 0 : (float)gc15TimeMs / gc15PeriodDurMs;
            assert gcFrac01 >= 0 && gcFrac01 <= 1 : "0 <= "+ gcFrac01 +" <= 1";
            assert gcFrac05 >= 0 && gcFrac05 <= 1 : "0 <= "+ gcFrac05 +" <= 1";
            assert gcFrac15 >= 0 && gcFrac15 <= 1 : "0 <= "+ gcFrac15 +" <= 1";
            log.info("GC time usage:  {}/min   {}/5min   {}/15min", gcFrac01, gcFrac05, gcFrac15);
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
         * <p>Example 1: If gcFrac05 is 0.25, this means that one fourth of the
         * execution time got into garbage collection.</p>
         *
         * <p>Example 2: If gcFrac15 is 0.03, this means that 3 percent of
         * execution time got into garbage collection.</p>
         */
        float gcFrac01 = NaN, gcFrac05 = NaN, gcFrac15 = NaN;

    }


}
