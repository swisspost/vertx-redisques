package org.swisspush.redisques;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.swisspush.redisques.util.DequeueStatistic;
import org.swisspush.redisques.util.DequeueStatisticCollector;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

@RunWith(VertxUnitRunner.class)
public class QueueStatsServiceTest {

    private Vertx vertx;
    private EventBus eventBus;
    private QueueStatsService queueStatsService;
    private RedisquesConfiguration config;
    private DequeueStatisticCollector dequeueStatisticCollector;

    /**
     * Test setup for cases where dequeue stats are enabled.
     */
    @Before
    public void setUpForEnabledDequeueStats() {
        vertx = Vertx.vertx();
        eventBus = Mockito.mock(EventBus.class);
        QueueStatisticsCollector queueStatisticsCollector = Mockito.mock(QueueStatisticsCollector.class);
        dequeueStatisticCollector = Mockito.mock(DequeueStatisticCollector.class);
        RedisQuesExceptionFactory exceptionFactory = Mockito.mock(RedisQuesExceptionFactory.class);
        Semaphore semaphore = new Semaphore(1);
        config = Mockito.spy(new RedisquesConfiguration());

        // Ensure fetchQueueStats is true
        queueStatsService = Mockito.spy(new QueueStatsService(vertx, eventBus, "redisques", queueStatisticsCollector,
                dequeueStatisticCollector, exceptionFactory, semaphore, true));
    }

    /**
     * Test setup for cases where dequeue stats are disabled.
     */
    @Before
    public void setUpForDisabledDequeueStats() {
        vertx = Vertx.vertx();
        eventBus = Mockito.mock(EventBus.class);
        QueueStatisticsCollector queueStatisticsCollector = Mockito.mock(QueueStatisticsCollector.class);
        dequeueStatisticCollector = Mockito.mock(DequeueStatisticCollector.class);
        RedisQuesExceptionFactory exceptionFactory = Mockito.mock(RedisQuesExceptionFactory.class);
        Semaphore semaphore = new Semaphore(1);
        config = Mockito.spy(new RedisquesConfiguration());

        // Ensure fetchQueueStats is false
        queueStatsService = Mockito.spy(new QueueStatsService(vertx, eventBus, "redisques", queueStatisticsCollector,
                dequeueStatisticCollector, exceptionFactory, semaphore, false));
    }

    /**
     * Test to verify that dequeue statistics are collected when the interval is greater than zero.
     */
    @Test
    public void testDequeueStatsCalledWhenIntervalGreaterThanZero(TestContext testContext) {
        setUpForEnabledDequeueStats();

        // Mock for enabling dequeue stats
        Mockito.when(config.isDequeueStatsEnabled()).thenReturn(true);

        // Mock method to return successful Future with dequeue statistics
        HashMap<String, DequeueStatistic> dequeueStatistics = new HashMap<>();
        dequeueStatistics.put("testQueue", new DequeueStatistic());
        Mockito.when(dequeueStatisticCollector.getAllDequeueStatistics()).thenReturn(Future.succeededFuture(dequeueStatistics));

        // Async setup for the test
        Async async = testContext.async();

        // Mock the context and mentor
        Object mockContext = new Object();
        QueueStatsService.GetQueueStatsMentor<Object> mentor = new QueueStatsService.GetQueueStatsMentor<>() {
            @Override
            public boolean includeEmptyQueues(Object ctx) {
                return true;
            }

            @Override
            public int limit(Object ctx) {
                return 10;
            }

            @Override
            public String filter(Object ctx) {
                return "*";
            }

            @Override
            public void onQueueStatistics(java.util.List<QueueStatsService.Queue> queues, Object ctx) {
                testContext.assertNotNull(queues);
                testContext.assertEquals(1, queues.size());
                async.complete();
            }

            @Override
            public void onError(Throwable ex, Object ctx) {
                testContext.fail(ex);
            }
        };

        // Mock fetchQueueNamesAndSize
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            request.queues = Collections.singletonList(new QueueStatsService.Queue("testQueue", 1));
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchQueueNamesAndSize(Mockito.any(), Mockito.any());

        // Mock fetchRetryDetails
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchRetryDetails(Mockito.any(), Mockito.any());

        // Call the method getQueueStats
        queueStatsService.getQueueStats(mockContext, mentor);

        // Capture arguments passed to attachDequeueStats
        ArgumentCaptor<QueueStatsService.GetQueueStatsRequest<Object>> requestCaptor = ArgumentCaptor.forClass(QueueStatsService.GetQueueStatsRequest.class);
        ArgumentCaptor<BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>>> consumerCaptor = ArgumentCaptor.forClass(BiConsumer.class);

        // Verify that attachDequeueStats was called
        Mockito.verify(queueStatsService).attachDequeueStats(requestCaptor.capture(), consumerCaptor.capture());

        // Verify that the request contains the correct queue
        testContext.assertNotNull(requestCaptor.getValue());
        testContext.assertEquals("testQueue", requestCaptor.getValue().queues.get(0).getName());

        // Verify that async completed correctly
        testContext.assertTrue(async.count() == 0);
    }

    /**
     * Test to verify that dequeue statistics are not collected when the interval is zero or negative.
     */
    @Test
    public void testDequeueStatsNotCalledWhenIntervalIsZeroOrNegative(TestContext testContext) {
        setUpForDisabledDequeueStats();

        // Mock the configuration to disable dequeue stats
        Mockito.when(config.isDequeueStatsEnabled()).thenReturn(false);

        // Async setup for the test
        Async async = testContext.async();

        // Mock the context and mentor
        Object mockContext = new Object();
        QueueStatsService.GetQueueStatsMentor<Object> mentor = new QueueStatsService.GetQueueStatsMentor<>() {
            @Override
            public boolean includeEmptyQueues(Object ctx) {
                return true;
            }

            @Override
            public int limit(Object ctx) {
                return 10;
            }

            @Override
            public String filter(Object ctx) {
                return "*";
            }

            @Override
            public void onQueueStatistics(java.util.List<QueueStatsService.Queue> queues, Object ctx) {
                // Ensure that nextDequeueDueTimestampEpochMs is null since stats are not attached
                testContext.assertTrue(queues.get(0).getNextDequeueDueTimestampEpochMs() == null);
                async.complete();
            }

            @Override
            public void onError(Throwable ex, Object ctx) {
                testContext.fail(ex);
            }
        };

        // Mock fetchQueueNamesAndSize
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            request.queues = Collections.singletonList(new QueueStatsService.Queue("testQueue", 1));
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchQueueNamesAndSize(Mockito.any(), Mockito.any());

        // Mock fetchRetryDetails
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchRetryDetails(Mockito.any(), Mockito.any());

        // Call the method getQueueStats
        queueStatsService.getQueueStats(mockContext, mentor);

        // Verify that attachDequeueStats was NOT called
        Mockito.verify(queueStatsService, Mockito.never()).attachDequeueStats(Mockito.any(), Mockito.any());

        // Verify that async completed correctly
        async.awaitSuccess();
    }
}
