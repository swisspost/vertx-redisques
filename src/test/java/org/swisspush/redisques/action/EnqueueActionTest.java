package org.swisspush.redisques.action;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.impl.types.BulkType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link EnqueueAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class EnqueueActionTest extends AbstractQueueActionTest {

    private Counter enqueueCounterSuccess;
    private Counter enqueueCounterFail;

    @Before
    @Override
    public void setup() {
        super.setup();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        enqueueCounterSuccess = meterRegistry.counter(MetricMeter.ENQUEUE_SUCCESS.getId(), MetricTags.IDENTIFIER.getId(), "foo");
        enqueueCounterFail = meterRegistry.counter(MetricMeter.ENQUEUE_FAIL.getId(), MetricTags.IDENTIFIER.getId(), "foo");
        action = new EnqueueAction(vertx, redisService, keyspaceHelper,
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class),
                Mockito.mock(Logger.class), memoryUsageProvider, 80, meterRegistry, "foo");
    }

    @Test
    public void testEnqueueWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"message\":\"RedisQues QUEUE_ERROR: Error while enqueueing message into queue someQueue\"}"))));
        verifyNoInteractions(redisAPI);

        assertEnqueueCounts(context,0.0, 1.0);
    }

    @Test
    public void testDontEnqueueWhenMemoryUsageLimitIsReached(TestContext context){
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));
        when(memoryUsageProvider.currentMemoryUsagePercentage()).thenReturn(Optional.of(85));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"message\":\"memory usage limit reached\"}"))));
        verifyNoInteractions(redisAPI);

        assertEnqueueCounts(context,0.0, 1.0);
    }

    @Test
    public void testDontEnqueueWhenUpdateTimestampFails(TestContext context){
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"updateTimestampFail\"},\"message\":\"hello\"}")));

        when(redisAPI.zadd(anyList()))
                .thenReturn(Future.failedFuture("Booom"));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":" +
                "\"error\",\"message\":\"RedisQues QUEUE_ERROR: Error while enqueueing message into " +
                "queue updateTimestampFail\"}"))));
        verify(redisAPI, never()).rpush(anyList());

        assertEnqueueCounts(context,0.0, 1.0);
    }

    @Test
    public void testEnqueueWhenUpdateTimestampSucceeds(TestContext context){
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));

        when(redisAPI.zadd(anyList()))
                .thenReturn(Future.succeededFuture());
        when(redisAPI.rpush(anyList())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer("1"), false)));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"message\":\"enqueued\"}"))));
        verify(redisAPI, times(1)).rpush(eq(Arrays.asList("prefix-someQueue", "hello")));

        assertEnqueueCounts(context,1.0, 0.0);
    }

    private void assertEnqueueCounts(TestContext context, double successCount, double failCount){
        context.assertEquals(successCount, enqueueCounterSuccess.count(), "Success enqueue count is wrong");
        context.assertEquals(failCount, enqueueCounterFail.count(), "Failed enqueue count is wrong");
    }
}
