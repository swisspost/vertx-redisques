package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for {@link RedisQuesTimer} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class RedisQuesTimerTest {

    private static final double TEST_BUFFER_MS = 2.5;

    @Test
    public void testExecuteDelayedLong(TestContext context){
        Async async = context.async();
        RedisQuesTimer timer = new RedisQuesTimer(Vertx.vertx());
        final int delayMs = 1500;
        final long start = System.currentTimeMillis();

        timer.executeDelayedMax(delayMs).onComplete(delayed -> {
            context.assertTrue(delayed.succeeded());

            long end = System.currentTimeMillis();
            long duration = end - start;
            assertMaxDuration(context, duration, delayMs);
            async.complete();
        });
    }

    @Test
    public void testExecuteDelayedShort(TestContext context){
        Async async = context.async();
        RedisQuesTimer timer = new RedisQuesTimer(Vertx.vertx());
        final int delayMs = 50;
        final long start = System.currentTimeMillis();

        timer.executeDelayedMax(delayMs).onComplete(delayed -> {
            context.assertTrue(delayed.succeeded());

            long end = System.currentTimeMillis();
            long duration = end - start;
            assertMaxDuration(context, duration, delayMs);
            async.complete();
        });
    }

    @Test
    public void testExecuteDelayedZero(TestContext context){
        Async async = context.async();
        RedisQuesTimer timer = new RedisQuesTimer(Vertx.vertx());
        final int delayMs = 0;
        final long start = System.currentTimeMillis();

        timer.executeDelayedMax(delayMs).onComplete(delayed -> {
            context.assertTrue(delayed.succeeded());

            long end = System.currentTimeMillis();
            long duration = end - start;
            assertMaxDuration(context, duration, delayMs);
            async.complete();
        });
    }

    private void assertMaxDuration(TestContext context, long duration, int delayMs){
        if(delayMs <= 0){
            delayMs = 1;
        }
        double delayPlus50Percent = delayMs * 1.5;
        if(delayPlus50Percent <= TEST_BUFFER_MS) {
            delayPlus50Percent = TEST_BUFFER_MS; // to increase test stability for very low delays we add some ms
        }
        context.assertTrue(duration <= delayPlus50Percent, "Future completed after " + duration + "ms. " +
                "However it should not have taken more than the delay + 50% which would be " + delayPlus50Percent + "ms");
    }
}
