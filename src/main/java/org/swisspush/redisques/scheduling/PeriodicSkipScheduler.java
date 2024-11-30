package org.swisspush.redisques.scheduling;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.lang.System.currentTimeMillis;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Same idea as {@link Vertx#setPeriodic(long, long, Handler)}. BUT prevents
 * tasks which start to overtake themselves.
 */
public class PeriodicSkipScheduler {

    private static final Logger log = getLogger(PeriodicSkipScheduler.class);
    private final Vertx vertx;

    public PeriodicSkipScheduler(Vertx vertx) {
        assert vertx != null;
        this.vertx = vertx;
    }

    /** Convenience overload for {@link #setPeriodic(long, long, String, Consumer)}. */
    public Timer setPeriodic(long periodMs, String dbgHint, Consumer<Runnable> task) {
        return setPeriodic(periodMs, periodMs, dbgHint, task);
    }

    /**
     * Same idea as {@link Vertx#setPeriodic(long, long, Handler)}. BUT prevents
     * tasks which start to overtake themselves.
     */
    public Timer setPeriodic(long initDelayMy, long periodMs, String dbgHint, Consumer<Runnable> task) {
        var timer = new Timer(task, dbgHint);
        timer.id = vertx.setPeriodic(initDelayMy, periodMs, timer::onTrigger_);
        return timer;
    }

    private void onTrigger(Timer timer) {
        if (!timer.isPreviousStillRunning.compareAndSet(false, true)) {
            log.debug("Have to skip run.");
            return;
        }
        Promise<Void> p = Promise.promise();
        var fut = p.future();
        fut.onSuccess((Void v) -> timer.onTaskDone_());
        fut.onFailure(ex -> log.error("This is expected to be UNREACHABLE ({})", timer.dbgHint, ex));
        try {
            timer.task.accept(p::complete);
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Task has failed ({})", timer.dbgHint, ex);
            } else {
                log.info("Task has failed ({}): {}", timer.dbgHint, ex.getMessage());
            }
            p.tryComplete();
        }
    }

    private void onTaskDone(Timer timer) {
        timer.isPreviousStillRunning.set(false);
    }

    private void cancel(Timer timer) {
        vertx.cancelTimer(timer.id);
    }


    public class Timer {
        private final Consumer<Runnable> task;
        private long id;
        private String dbgHint;
        private volatile AtomicBoolean isPreviousStillRunning;

        private Timer(Consumer<Runnable> task, String dbgHint) {
            this.task = task;
            this.dbgHint = dbgHint;
            isPreviousStillRunning = new AtomicBoolean(false);
        }
        private void onTrigger_(Long aLong) { onTrigger(this); }
        private void onTaskDone_() { onTaskDone(this); }
        public void cancel() { PeriodicSkipScheduler.this.cancel(this); }
    }

}
