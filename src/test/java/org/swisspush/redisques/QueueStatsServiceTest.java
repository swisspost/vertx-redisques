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

    @Before
    public void setUp() {
        // Initialize Vertx and mock dependencies
        vertx = Vertx.vertx();
        eventBus = Mockito.mock(EventBus.class);
        QueueStatisticsCollector queueStatisticsCollector = Mockito.mock(QueueStatisticsCollector.class);
        dequeueStatisticCollector = Mockito.mock(DequeueStatisticCollector.class);
        RedisQuesExceptionFactory exceptionFactory = Mockito.mock(RedisQuesExceptionFactory.class);
        Semaphore semaphore = new Semaphore(1);
        config = Mockito.spy(new RedisquesConfiguration());

        // Create the QueueStatsService instance with mocked dependencies
        queueStatsService = Mockito.spy(new QueueStatsService(vertx, eventBus, "redisques", queueStatisticsCollector,
                dequeueStatisticCollector, exceptionFactory, semaphore, config));
    }

    @Test
    public void testDequeueStatsCalledWhenIntervalGreaterThanZero(TestContext testContext) {
        // Mock the config so that getDequeueStatisticReportIntervalSec returns a value greater than 0
        Mockito.when(config.getDequeueStatisticReportIntervalSec()).thenReturn(10); // Value greater than 0

        // Mock the getAllDequeueStatistics method to return a successful Future
        HashMap<String, DequeueStatistic> dequeueStatistics = new HashMap<>();
        dequeueStatistics.put("testQueue", new DequeueStatistic());
        Mockito.when(dequeueStatisticCollector.getAllDequeueStatistics()).thenReturn(Future.succeededFuture(dequeueStatistics));

        // Set up async for the test
        Async async = testContext.async();

        // Mock a context and mentor
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
                // Assert the dequeue stats were attached
                testContext.assertTrue(queues.get(0).getSize() >0);
                async.complete();
            }

            @Override
            public void onError(Throwable ex, Object ctx) {
                testContext.fail(ex);
            }
        };

        // Mock fetchQueueNamesAndSize to return a valid list of queues
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            request.queues = Collections.singletonList(new QueueStatsService.Queue("testQueue", 1));
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchQueueNamesAndSize(Mockito.any(), Mockito.any());

        // Mock fetchRetryDetails to do nothing and call the next step
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchRetryDetails(Mockito.any(), Mockito.any());

        // Call getQueueStats to trigger the flow
        queueStatsService.getQueueStats(mockContext, mentor);

        // Use ArgumentCaptor to capture the arguments passed to attachDequeueStats
        ArgumentCaptor<QueueStatsService.GetQueueStatsRequest<Object>> requestCaptor = ArgumentCaptor.forClass(QueueStatsService.GetQueueStatsRequest.class);
        ArgumentCaptor<BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>>> consumerCaptor = ArgumentCaptor.forClass(BiConsumer.class);

        // Verify that attachDequeueStats was called with the correct arguments
        Mockito.verify(queueStatsService).attachDequeueStats(requestCaptor.capture(), consumerCaptor.capture());

        // Assert that the arguments are not null and match expectations
        testContext.assertNotNull(requestCaptor.getValue());
        testContext.assertNotNull(consumerCaptor.getValue());
    }

    @Test
    public void testDequeueStatsNotCalledWhenIntervalIsZeroOrNegative(TestContext testContext) {
        // Mock the config so that getDequeueStatisticReportIntervalSec returns 0 or a negative value
        Mockito.when(config.getDequeueStatisticReportIntervalSec()).thenReturn(0); // Value equal to 0
        // Alternatively: Mockito.when(config.getDequeueStatisticReportIntervalSec()).thenReturn(-1); // For negative value

        // Set up async for the test
        Async async = testContext.async();

        // Mock a context and mentor
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
                // Assert the dequeue stats were not attached
                testContext.assertTrue(queues.get(0).getNextDequeueDueTimestampEpochMs() == null);
                async.complete();
            }

            @Override
            public void onError(Throwable ex, Object ctx) {
                testContext.fail(ex);
            }
        };

        // Mock fetchQueueNamesAndSize to return a valid list of queues
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            request.queues = Collections.singletonList(new QueueStatsService.Queue("testQueue", 1));
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchQueueNamesAndSize(Mockito.any(), Mockito.any());

        // Mock fetchRetryDetails to do nothing and call the next step
        Mockito.doAnswer(invocation -> {
            BiConsumer<Throwable, QueueStatsService.GetQueueStatsRequest<Object>> callback = invocation.getArgument(1);
            QueueStatsService.GetQueueStatsRequest<Object> request = invocation.getArgument(0);
            callback.accept(null, request);
            return null;
        }).when(queueStatsService).fetchRetryDetails(Mockito.any(), Mockito.any());

        // Call getQueueStats to trigger the flow
        queueStatsService.getQueueStats(mockContext, mentor);

        // Verify that attachDequeueStats was NOT called
        Mockito.verify(queueStatsService, Mockito.never()).attachDequeueStats(Mockito.any(), Mockito.any());

        async.complete();
    }
}
