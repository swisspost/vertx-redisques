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
     * Broken path: RedisQues passed its own meterRegistry field (null when no registry injected
     * via builder) directly to QueueActionsService. EnqueueAction guards counter registration
     * behind "if (meterRegistry != null)", so with null the counter is never created.
     *
     * Fixed path: RedisQues.initialize() now reads back the resolved registry via
     * queueMetrics.getMeterRegistry() after initMicrometerMetrics() (which applies the
     * BackendRegistries fallback) and passes that non-null registry to QueueActionsService.
     */
    @Test
    public void testEnqueueSuccessMetricRegression(TestContext context) {
        when(keyspaceHelper.getConsumersAddress()).thenReturn("address-consumers");
        when(redisAPI.rpush(anyList())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer("1"), false)));
        when(registryService.updateTimestamp(anyString())).thenReturn(Future.succeededFuture(null));
        when(registryService.notifyConsumer(anyString())).thenReturn(Future.succeededFuture());

        SimpleMeterRegistry brokenRegistry = new SimpleMeterRegistry();
        Counter brokenCounter = brokenRegistry.counter(MetricMeter.ENQUEUE_SUCCESS.getId(), MetricTags.IDENTIFIER.getId(), "foo");

        EnqueueAction brokenAction = new EnqueueAction(vertx, registryService, redisService, keyspaceHelper,
                queueConfigurationProvider, getConfigurationProvider(), exceptionFactory,
                Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class),
                memoryUsageProvider, null);
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer(
                "{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));
        brokenAction.execute(message);

        context.assertEquals(0.0, brokenCounter.count(),
                "Broken path: ENQUEUE_SUCCESS must stay at 0 when EnqueueAction received null MeterRegistry");

        SimpleMeterRegistry fixedRegistry = new SimpleMeterRegistry();
        Counter fixedCounter = fixedRegistry.counter(MetricMeter.ENQUEUE_SUCCESS.getId(), MetricTags.IDENTIFIER.getId(), "foo");

        EnqueueAction fixedAction = new EnqueueAction(vertx, registryService, redisService, keyspaceHelper,
                queueConfigurationProvider, getConfigurationProvider(), exceptionFactory,
                Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class),
                memoryUsageProvider, fixedRegistry);
        when(message.body()).thenReturn(new JsonObject(Buffer.buffer(
                "{\"operation\":\"enqueue\",\"payload\":{\"queuename\":\"someQueue\"},\"message\":\"hello\"}")));
        fixedAction.execute(message);

        context.assertEquals(1.0, fixedCounter.count(),
                "Fixed path: ENQUEUE_SUCCESS must be incremented when EnqueueAction receives the registry from queueMetrics.getMeterRegistry()");
    }

    private void assertEnqueueCounts(TestContext context, double successCount, double failCount) {
        context.assertEquals(successCount, enqueueCounterSuccess.count(), "Success enqueue count is wrong");
        context.assertEquals(failCount, enqueueCounterFail.count(), "Failed enqueue count is wrong");
    }
}
