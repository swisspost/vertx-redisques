package org.swisspush.redisques.scheduling;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.lang.System.currentTimeMillis;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Same idea as {@link Vertx#setPeriodic(long, long, Handler)}. BUT prevents
 * tasks which start to overtake themself.
 */
public class PeriodicSkipScheduler {

    private static final Logger log = getLogger(PeriodicSkipScheduler.class);
    private final Vertx vertx;

    public PeriodicSkipScheduler(Vertx vertx) {
        assert vertx != null;
        this.vertx = vertx;
    }

    /** Convenience overload for {@link #setPeriodic(long, long, Consumer)}. */
    public Timer setPeriodic(long periodMs, Consumer<Runnable> task) {
        return setPeriodic(periodMs, periodMs, task);
    }

    /**
     * Same idea as {@link Vertx#setPeriodic(long, long, Handler)}. BUT prevents
     * tasks which start to overtake themself.
     */
    public Timer setPeriodic(long initDelayMy, long periodMs, Consumer<Runnable> task) {
        var timer = new Timer(task);
        timer.id = vertx.setPeriodic(initDelayMy, periodMs, timer::onTrigger_);
        return timer;
    }

    private void onTrigger(Timer timer) {
        long now = currentTimeMillis();
        boolean isPreviousStillRunning = timer.begEpochMs > timer.endEpochMs;
        if (isPreviousStillRunning) {
            log.debug("Previous task still running. We have to NOT run in this interval. Previous did not respond for {}ms.",
                    now - timer.begEpochMs);
            return;
        }
        timer.begEpochMs = currentTimeMillis();
        boolean oldValue = timer.isPending.getAndSet(true);
        assert !oldValue : "Why is this already pending?";
        timer.task.accept(timer::onTaskDone_);
    }

    private void onTaskDone(Timer timer) {
        boolean oldVal = timer.isPending.getAndSet(false);
        if (!oldVal) {
            throw new IllegalStateException("MUST NOT be called multiple times!");
        }
        timer.endEpochMs = currentTimeMillis();
    }

    private void cancel(Timer timer) {
        vertx.cancelTimer(timer.id);
    }


    public class Timer {
        private final Consumer<Runnable> task;
        private long id;
        // When the last run has begun and end.
        private long begEpochMs, endEpochMs;
        private final AtomicBoolean isPending = new AtomicBoolean();

        private Timer(Consumer<Runnable> task) {
            this.task = task;
        }
        private void onTrigger_(Long aLong) { onTrigger(this); }
        private void onTaskDone_() { onTaskDone(this); }
        public void cancel() { PeriodicSkipScheduler.this.cancel(this); }
    }

}
