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
import org.swisspush.redisques.queue.QueueConsumerRunner;
import org.swisspush.redisques.queue.QueueRegistryService;
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.Arrays;
import java.util.HashMap;
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
    private MeterRegistry meterRegistry;
    private QueueRegistryService registryService;
    private QueueConsumerRunner runner;
    private QueueConfigurationProvider queueConfigurationProvider = Mockito.mock(QueueConfigurationProvider.class);

    @Before
    @Override
    public void setup() {
        super.setup();
        registryService = Mockito.mock(QueueRegistryService.class);
        runner = Mockito.mock(QueueConsumerRunner.class);
        when(registryService.getQueueConsumerRunner()).thenReturn(runner);
        when(runner.getMyQueues()).thenReturn(new HashMap<>());
        meterRegistry = new SimpleMeterRegistry();
        enqueueCounterSuccess = meterRegistry.counter(MetricMeter.ENQUEUE_SUCCESS.getId(), MetricTags.IDENTIFIER.getId(), "foo");
        enqueueCounterFail = meterRegistry.counter(MetricMeter.ENQUEUE_FAIL.getId(), MetricTags.IDENTIFIER.getId(), "foo");
        action = new EnqueueAction(vertx, registryService, redisService, keyspaceHelper,
                queueConfigurationProvider, getConfigurationProvider(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class),
                Mockito.mock(Logger.class), memoryUsageProvider, meterRegistry);

    }

    @Test
    public void testEnqueueWhenRedisIsNotReady(TestContext context) {
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));
        when(registryService.updateTimestamp(anyString())).thenReturn(Future.succeededFuture(null));
        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"message\":\"RedisQues QUEUE_ERROR: Error while enqueueing message into queue someQueue\"}"))));
        verifyNoInteractions(redisAPI);

        assertEnqueueCounts(context, 0.0, 1.0);
    }


    @Test
    public void testDontEnqueueWhenMemoryUsageLimitIsReached(TestContext context) {
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));
        when(memoryUsageProvider.currentMemoryUsagePercentage()).thenReturn(Optional.of(85));
        when(registryService.updateTimestamp(anyString())).thenReturn(Future.succeededFuture(null));
        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"message\":\"memory usage limit reached\"}"))));
        verifyNoInteractions(redisAPI);

        assertEnqueueCounts(context, 0.0, 1.0);
    }

    @Test
    public void testDontEnqueueWhenUpdateTimestampFails(TestContext context) {
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"updateTimestampFail\"},\"message\":\"hello\"}")));
        when(registryService.notifyConsumer(anyString())).thenReturn(Future.succeededFuture());
        when(registryService.updateTimestamp(anyString())).thenReturn(Future.failedFuture("Booom"));
        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":" +
                "\"error\",\"message\":\"RedisQues QUEUE_ERROR: Error while enqueueing message into " +
                "queue updateTimestampFail\"}"))));
        verify(redisAPI, never()).rpush(anyList());

        assertEnqueueCounts(context, 0.0, 1.0);
    }

    @Test
    public void testEnqueueWhenUpdateTimestampSucceeds(TestContext context) {
        when(keyspaceHelper.getConsumersAddress()).thenReturn("addrsss" + "-consumers");
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer("{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));
        when(redisAPI.get(any()))
                .thenReturn(Future.succeededFuture());
        when(redisAPI.zadd(anyList()))
                .thenReturn(Future.succeededFuture());
        when(redisAPI.rpush(anyList())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer("1"), false)));
        when(registryService.updateTimestamp(anyString())).thenReturn(Future.succeededFuture(null));
        when(registryService.notifyConsumer(anyString())).thenReturn(Future.succeededFuture());
        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"message\":\"enqueued\"}"))));
        verify(redisAPI, times(1)).rpush(eq(Arrays.asList("prefix-someQueue", "hello")));

        assertEnqueueCounts(context, 1.0, 0.0);
    }

    /**
     * Regression test for https://github.com/swisspost/vertx-redisques/issues/406: ENQUEUE_SUCCESS metric missing after upgrade to v4.1.33.
     *
     * Root cause: RedisQues.initialize() passes its own meterRegistry field (possibly null) to
     * QueueActionsService → QueueActionFactory → EnqueueAction. The BackendRegistries fallback
     * that resolves a default registry was moved into QueueMetrics.initMicrometerMetrics() and
     * stays local to that object, so EnqueueAction never receives a non-null registry when the
     * caller does not explicitly inject one via the builder.
     *
     * EnqueueAction only registers its counters when meterRegistry != null, so ENQUEUE_SUCCESS
     * is silently absent despite a successful enqueue.
     *
     * This test documents the broken state: the counter stays at 0.0 even though the enqueue
     * succeeds. Once the fix is applied (apply the BackendRegistries fallback in
     * RedisQues.initialize() before constructing QueueActionsService), change the expected
     * counter value from 0.0 to 1.0.
     */
    @Test
    public void testEnqueueSuccessMetricMissingWhenMeterRegistryIsNull(TestContext context) {
        EnqueueAction actionWithNullRegistry = new EnqueueAction(
                vertx, registryService, redisService, keyspaceHelper,
                queueConfigurationProvider, getConfigurationProvider(), exceptionFactory,
                Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class),
                memoryUsageProvider, null);

        when(keyspaceHelper.getConsumersAddress()).thenReturn("address-consumers");
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer(
                "{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));
        when(redisAPI.rpush(anyList())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer("1"), false)));
        when(registryService.updateTimestamp(anyString())).thenReturn(Future.succeededFuture(null));
        when(registryService.notifyConsumer(anyString())).thenReturn(Future.succeededFuture());

        actionWithNullRegistry.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"message\":\"enqueued\"}"))));

        double enqueueSuccessCount = meterRegistry.counter(
                MetricMeter.ENQUEUE_SUCCESS.getId(), MetricTags.IDENTIFIER.getId(), "foo").count();
        context.assertEquals(0.0, enqueueSuccessCount,
                "REGRESSION (https://github.com/swisspost/vertx-redisques/issues/406): ENQUEUE_SUCCESS should have been incremented but was not " +
                "because EnqueueAction received a null MeterRegistry. " +
                "Update this assertion to 1.0 once the fix is applied.");
    }

    private void assertEnqueueCounts(TestContext context, double successCount, double failCount) {
        context.assertEquals(successCount, enqueueCounterSuccess.count(), "Success enqueue count is wrong");
        context.assertEquals(failCount, enqueueCounterFail.count(), "Failed enqueue count is wrong");
    }
}
