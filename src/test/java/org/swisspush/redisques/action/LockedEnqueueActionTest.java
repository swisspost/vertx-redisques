package org.swisspush.redisques.action;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildLockedEnqueueOperation;

/**
 * Tests for {@link LockedEnqueueAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class LockedEnqueueActionTest extends AbstractQueueActionTest {

    private Counter enqueueCounterSuccess;
    private Counter enqueueCounterFail;

    @Before
    @Override
    public void setup() {
        super.setup();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        enqueueCounterSuccess = meterRegistry.counter(MetricMeter.ENQUEUE_SUCCESS.getId(), MetricTags.IDENTIFIER.getId(), "foo");
        enqueueCounterFail = meterRegistry.counter(MetricMeter.ENQUEUE_FAIL.getId(), MetricTags.IDENTIFIER.getId(), "foo");
        action = new LockedEnqueueAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class),
                Mockito.mock(Logger.class), memoryUsageProvider, 80, meterRegistry, "foo");
    }

    @Test
    public void testLockedEnqueueWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildLockedEnqueueOperation("queueEnqueue", "helloEnqueue", "someuser"));

        action.execute(message);

        assertEnqueueCounts(context, 0.0, 1.0);
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\"}"))));
        verifyNoInteractions(redisAPI);
    }

    private void assertEnqueueCounts(TestContext context, double successCount, double failCount){
        context.assertEquals(successCount, enqueueCounterSuccess.count(), "Success enqueue count is wrong");
        context.assertEquals(failCount, enqueueCounterFail.count(), "Failed enqueue count is wrong");
    }
}
