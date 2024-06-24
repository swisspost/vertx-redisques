package org.swisspush.redisques.performance;

import io.vertx.core.Vertx;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class BurstSquasher<Ctx> {

    private final Vertx vertx;
    private final Action<Ctx> action;
    private final Lock mutx = new ReentrantLock();
    private final int minDelayMs, maxDelayMs;
    private long prevPublishEpchMs;
    private int count;
    private boolean maxCheckerIsRunning;
    private Ctx mostRecentCtx;

    public BurstSquasher(Vertx vertx, Action<Ctx> action) {
        this(vertx, action, 1000, 10000);
    }

    public BurstSquasher(Vertx vertx, Action<Ctx> action, int minDelayMs, int maxDelayMs) {
        assert vertx != null;
        assert action != null;
        assert minDelayMs >= 1000;
        assert maxDelayMs <= 86400000;
        this.vertx = vertx;
        this.action = action;
        this.minDelayMs = max(1000, minDelayMs);
        this.maxDelayMs = Math.min(maxDelayMs, 86400000);
    }

    private void maxLimitCheck(Long nonsesne) {
        boolean isPublish = false, maxCheckerNeedsToRunAgain = false;
        long durationSincePrevPublishMs;
        int countLocalCpy = -1;
        Ctx mostRecentCtxLocalCpy = null;
        long now = currentTimeMillis();
        mutx.lock();
        try {
            maxCheckerIsRunning = false;
            durationSincePrevPublishMs = now - prevPublishEpchMs;
            if (count > 0 && durationSincePrevPublishMs >= maxDelayMs) {
                isPublish = true;
                prevPublishEpchMs = now;
                countLocalCpy = count;
                mostRecentCtxLocalCpy = mostRecentCtx;
                count = 0;
                mostRecentCtx = null;
            } else if (count > 0) {
                maxCheckerIsRunning = true;
                maxCheckerNeedsToRunAgain = true;
            }
        } finally {
            mutx.unlock();
        }
        if (maxCheckerNeedsToRunAgain) {
            vertx.setTimer(max(1000, maxDelayMs - durationSincePrevPublishMs + 1), this::maxLimitCheck);
        }
        if (isPublish) {
            action.perform(countLocalCpy, mostRecentCtxLocalCpy);
        }
    }

    public void logSomewhen(Ctx ctx) {
        boolean isPublish = false, maxCheckerNeedsStart = false;
        int countLocalCpy = -1;
        Ctx mostRecentCtxLocalCpy = null;
        long durationSincePrevPublishMs;
        long now = currentTimeMillis();
        mutx.lock();
        try {
            mostRecentCtx = ctx;
            count += 1;
            durationSincePrevPublishMs = now - prevPublishEpchMs;
            if (durationSincePrevPublishMs > minDelayMs) {
                isPublish = true;
                prevPublishEpchMs = now;
                countLocalCpy = count;
                mostRecentCtxLocalCpy = mostRecentCtx;
                count = 0;
                mostRecentCtx = null;
            } else if (!maxCheckerIsRunning) {
                maxCheckerNeedsStart = true;
                maxCheckerIsRunning = true;
            }
        } finally {
            mutx.unlock();
        }
        if (maxCheckerNeedsStart) {
            vertx.setTimer((Math.min(1000, maxDelayMs - durationSincePrevPublishMs + 1)), this::maxLimitCheck);
        }
        if (isPublish) {
            action.perform(countLocalCpy, mostRecentCtxLocalCpy);
        }
    }


    public static interface Action<Ctx> {
        public void perform(int count, Ctx ctx);
    }

}
