package org.swisspush.redisques.queue;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.redis.client.Response;
import org.junit.Before;
import org.junit.Test;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.QueueStatsService;
import org.swisspush.redisques.util.MessageConsumerManager;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;

public class QueueConsumerRunnerFailureTest {
    private static final String QUEUE = "failure-queue";
    private static final String QUEUE_KEY = "queues:" + QUEUE;
    private static final String CONSUMER_ID = "consumer-id";
    private static final String NOTIFY_ADDRESS = "notify-consumer";
    private static final String FAILURE_MESSAGE = "Redis waiting queue is full";

    private Vertx vertx;
    private EventBus eventBus;
    private RedisService redisService;
    private QueueMetrics metrics;
    private QueueStatsService queueStatsService;
    private QueueStatisticsCollector statisticsCollector;
    private QueueConfigurationProvider queueConfigurationProvider;
    private QueueConsumerRunner runner;
    private Handler<Long> retryHandler;

    @Before
    public void setUp() {
        vertx = mock(Vertx.class);
        eventBus = mock(EventBus.class);
        redisService = mock(RedisService.class);
        metrics = mock(QueueMetrics.class);
        queueStatsService = mock(QueueStatsService.class);
        statisticsCollector = mock(QueueStatisticsCollector.class);
        queueConfigurationProvider = mock(QueueConfigurationProvider.class);
        KeyspaceHelper keyspaceHelper = mock(KeyspaceHelper.class);
        RedisquesConfigurationProvider configurationProvider = mock(RedisquesConfigurationProvider.class);
        RedisquesConfiguration configuration = mock(RedisquesConfiguration.class);

        when(configurationProvider.configuration()).thenReturn(configuration);
        when(configuration.getRefreshPeriod()).thenReturn(2);
        when(configuration.getConsumerLockMultiplier()).thenReturn(2);
        when(keyspaceHelper.getVerticleUid()).thenReturn(CONSUMER_ID);
        when(keyspaceHelper.getQueuesPrefix()).thenReturn("queues:");
        when(keyspaceHelper.getConsumersPrefix()).thenReturn("consumers:");
        when(keyspaceHelper.getLocksKey()).thenReturn("locks");
        when(keyspaceHelper.getVerticleNotifyConsumerKey()).thenReturn(NOTIFY_ADDRESS);
        when(vertx.eventBus()).thenReturn(eventBus);
        when(vertx.setTimer(anyLong(), any())).thenAnswer(invocation -> {
            retryHandler = invocation.getArgument(1);
            return 1L;
        });
        Response consumer = stringResponse(CONSUMER_ID);
        when(redisService.batch(anyList())).thenReturn(
                Future.succeededFuture(Arrays.asList(null, consumer)));
        when(redisService.hexists("locks", QUEUE)).thenReturn(Future.succeededFuture());
        when(statisticsCollector.queueMessageFailed(QUEUE)).thenReturn(1L);
        when(eventBus.request(NOTIFY_ADDRESS, QUEUE)).thenReturn(Future.succeededFuture());

        runner = new QueueConsumerRunner(vertx, redisService, metrics, queueStatsService, keyspaceHelper,
                configurationProvider, newWastefulExceptionFactory(), statisticsCollector,
                queueConfigurationProvider, mock(MessageConsumerManager.class));
        runner.setMyQueuesState(QUEUE, QueueState.READY);
    }

    @Test
    public void consumeCompletesWhenRegistrationRefreshFails() {
        when(redisService.batch(anyList())).thenReturn(Future.failedFuture(FAILURE_MESSAGE));

        assertRegistrationFailureCompletes();
    }

    @Test
    public void consumeCompletesWhenRegistrationResponseIsNull() {
        when(redisService.batch(anyList())).thenReturn(Future.succeededFuture());

        assertRegistrationFailureCompletes();
    }

    @Test
    public void consumeCompletesWhenRegistrationResponseIsEmpty() {
        assertInvalidRegistrationResponse(Collections.emptyList());
    }

    @Test
    public void consumeCompletesWhenRegistrationResponseHasTooFewEntries() {
        assertInvalidRegistrationResponse(Collections.singletonList(stringResponse("1")));
    }

    @Test
    public void consumeCompletesWhenRegistrationResponseHasTooManyEntries() {
        assertInvalidRegistrationResponse(Arrays.asList(
                stringResponse("1"), stringResponse(CONSUMER_ID), stringResponse("extra")));
    }

    @Test
    public void consumeCompletesWhenRegistrationHasNoConsumer() {
        Response expiration = stringResponse("1");
        when(redisService.batch(anyList())).thenReturn(
                Future.succeededFuture(Arrays.asList(expiration, null)));

        assertCompletedSuccessfully(runner.consume(QUEUE));

        assertEquals(QueueState.READY, state().getState());
        assertTrue(state().getLastRegisterRefreshedMillis() > 0);
        verifyNoInteractions(metrics, statisticsCollector, eventBus);
        verify(redisService, never()).hexists(anyString(), anyString());
        verify(vertx, never()).setTimer(anyLong(), any());
    }

    @Test
    public void singleItemReadFailureSchedulesRetryAndQueueCanBeConsumedAgain() {
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(
                Future.failedFuture(FAILURE_MESSAGE), Future.succeededFuture());

        assertCompletedSuccessfully(runner.consume(QUEUE));

        assertRetryScheduled(2000L);
        verifyNoInteractions(eventBus);
        retryHandler.handle(1L);

        assertEquals(QueueState.READY, state().getState());
        verify(eventBus).request(NOTIFY_ADDRESS, QUEUE);
        verify(queueStatsService).dequeueStatisticSetNextDequeueDueTimestamp(
                eq(QUEUE), anyLong(), eq("readQueue failed: " + FAILURE_MESSAGE));

        assertCompletedSuccessfully(runner.consume(QUEUE));

        verify(redisService, times(2)).lindex(QUEUE_KEY, "0");
        verify(statisticsCollector).queueMessageFailed(QUEUE);
        verify(queueStatsService).dequeueStatisticMarkedForRemoval(QUEUE);
        assertEquals(QueueState.READY, state().getState());
    }

    @Test
    public void readFailureUsesConfiguredRetryInterval() {
        when(queueConfigurationProvider.findRetryIntervalConfig(QUEUE)).thenReturn(List.of(3, 7));
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(Future.failedFuture(FAILURE_MESSAGE));

        assertCompletedSuccessfully(runner.consume(QUEUE));

        assertRetryScheduled(3000L);
        verify(statisticsCollector).setQueueSlowDownTime(QUEUE, 3);
    }

    @Test
    public void consumeDuringRetryDelayDoesNotReadOrScheduleAnotherRetry() {
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(Future.failedFuture(FAILURE_MESSAGE));

        assertCompletedSuccessfully(runner.consume(QUEUE));
        assertCompletedSuccessfully(runner.consume(QUEUE));

        assertRetryScheduled(2000L);
        verify(redisService).lindex(QUEUE_KEY, "0");
        verify(vertx, times(1)).setTimer(anyLong(), any());
        verifyNoInteractions(eventBus);
    }

    @Test
    public void repeatedReadFailuresIncreaseRetryInterval() {
        when(queueConfigurationProvider.findRetryIntervalConfig(QUEUE)).thenReturn(List.of(3, 7));
        when(statisticsCollector.queueMessageFailed(QUEUE)).thenReturn(1L, 2L);
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(Future.failedFuture(FAILURE_MESSAGE));

        assertCompletedSuccessfully(runner.consume(QUEUE));
        assertRetryScheduled(3000L);
        retryHandler.handle(1L);
        assertEquals(QueueState.READY, state().getState());

        assertCompletedSuccessfully(runner.consume(QUEUE));

        assertEquals(QueueState.CONSUMING, state().getState());
        verify(vertx).setTimer(eq(7000L), any());
        verify(statisticsCollector, times(2)).queueMessageFailed(QUEUE);
        verify(statisticsCollector).setQueueSlowDownTime(QUEUE, 7);
        retryHandler.handle(1L);
        assertEquals(QueueState.READY, state().getState());
    }

    @Test
    public void batchReadFailureSchedulesRetry() {
        configureBatch(0);
        when(redisService.lrange(QUEUE_KEY, "0", "2")).thenReturn(Future.failedFuture(FAILURE_MESSAGE));

        assertCompletedSuccessfully(runner.consume(QUEUE));

        assertRetryScheduled(2000L);
        verify(redisService, never()).lindex(anyString(), anyString());
        retryHandler.handle(1L);
        assertEquals(QueueState.READY, state().getState());
    }

    @Test
    public void batchSizeReadFailureSchedulesRetry() {
        configureBatch(2);
        when(redisService.llen(QUEUE_KEY)).thenReturn(Future.failedFuture(FAILURE_MESSAGE));

        assertCompletedSuccessfully(runner.consume(QUEUE));

        assertRetryScheduled(2000L);
        verify(redisService, never()).lrange(anyString(), anyString(), anyString());
        retryHandler.handle(1L);
        assertEquals(QueueState.READY, state().getState());
    }

    @Test
    public void failedRetryNotificationStillResetsQueueToReady() {
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(Future.failedFuture(FAILURE_MESSAGE));
        when(eventBus.request(NOTIFY_ADDRESS, QUEUE)).thenReturn(Future.failedFuture("notification failed"));

        assertCompletedSuccessfully(runner.consume(QUEUE));
        assertRetryScheduled(2000L);
        retryHandler.handle(1L);

        verify(eventBus).request(NOTIFY_ADDRESS, QUEUE);
        assertEquals(QueueState.READY, state().getState());
    }

    @Test
    public void retryWaitsForNotificationToCompleteBeforeResettingState() {
        Promise<Message<Object>> notification = Promise.promise();
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(Future.failedFuture(FAILURE_MESSAGE));
        when(eventBus.request(NOTIFY_ADDRESS, QUEUE)).thenReturn(notification.future());

        assertCompletedSuccessfully(runner.consume(QUEUE));
        assertRetryScheduled(2000L);
        retryHandler.handle(1L);
        assertEquals(QueueState.CONSUMING, state().getState());

        notification.complete();

        assertEquals(QueueState.READY, state().getState());
    }

    @Test
    public void readFailureDoesNotScheduleRetryWhenQueueIsAlreadyReady() {
        Promise<Response> read = Promise.promise();
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(read.future());

        Future<Void> consume = runner.consume(QUEUE);
        assertFalse(consume.isComplete());
        assertEquals(QueueState.CONSUMING, state().getState());
        runner.setMyQueuesState(QUEUE, QueueState.READY);
        read.fail(FAILURE_MESSAGE);

        assertCompletedSuccessfully(consume);
        assertEquals(QueueState.READY, state().getState());
        verifyNoInteractions(statisticsCollector, eventBus);
        verify(vertx, never()).setTimer(anyLong(), any());
    }

    @Test
    public void readFailureDoesNotScheduleRetryWhenQueueIsNoLongerOwned() {
        Promise<Response> read = Promise.promise();
        when(redisService.lindex(QUEUE_KEY, "0")).thenReturn(read.future());

        Future<Void> consume = runner.consume(QUEUE);
        assertFalse(consume.isComplete());
        runner.getMyQueues().remove(QUEUE);
        read.fail(FAILURE_MESSAGE);

        assertCompletedSuccessfully(consume);
        assertFalse(runner.getMyQueues().containsKey(QUEUE));
        verifyNoInteractions(statisticsCollector, eventBus);
        verify(vertx, never()).setTimer(anyLong(), any());
    }

    private void assertInvalidRegistrationResponse(List<Response> responses) {
        when(redisService.batch(anyList())).thenReturn(Future.succeededFuture(responses));
        assertRegistrationFailureCompletes();
    }

    private void assertRegistrationFailureCompletes() {
        assertCompletedSuccessfully(runner.consume(QUEUE));
        assertEquals(QueueState.READY, state().getState());
        assertEquals(0L, state().getLastRegisterRefreshedMillis());
        verifyNoInteractions(metrics, statisticsCollector, eventBus);
        verify(redisService, never()).hexists(anyString(), anyString());
        verify(vertx, never()).setTimer(anyLong(), any());
    }

    private void assertRetryScheduled(long delayMillis) {
        assertEquals(QueueState.CONSUMING, state().getState());
        verify(statisticsCollector).queueMessageFailed(QUEUE);
        verify(vertx).setTimer(eq(delayMillis), any());
        assertNotNull(retryHandler);
    }

    private void configureBatch(int minimumItems) {
        QueueConfigurationProvider.BatchQueueItemsConfig batch = new QueueConfigurationProvider.BatchQueueItemsConfig();
        batch.maximumItemInBatchDispatch = 3;
        batch.minimumItemInBatchDispatch = minimumItems;
        when(queueConfigurationProvider.findBatchQueueItemsConfig(QUEUE)).thenReturn(batch);
    }

    private QueueProcessingState state() {
        return runner.getMyQueues().get(QUEUE);
    }

    private static void assertCompletedSuccessfully(Future<Void> future) {
        assertTrue("consume must complete instead of hanging", future.isComplete());
        assertTrue("consume must succeed, but failed with " + future.cause(), future.succeeded());
    }

    private static Response stringResponse(String value) {
        Response response = mock(Response.class);
        when(response.toString()).thenReturn(value);
        return response;
    }
}
