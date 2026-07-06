package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.QueueProcessingState;
import org.swisspush.redisques.queue.RedisService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link RedisQuesTimer} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class QueueStatisticsCollectorTest {
    private Vertx vertx;
    private QueueStatisticsCollector queueStatisticsCollector;
    private RedisService redisService;
    private KeyspaceHelper keyspaceHelper;
    private RedisQuesExceptionFactory exceptionFactory;
    private Semaphore redisRequestQuota;
    private RedisquesConfigurationProvider configurationProvider;


    @Before
    public void before(TestContext context) {
        this.vertx = Vertx.vertx();
        this.redisService = Mockito.mock(RedisService.class);
        this.keyspaceHelper = Mockito.mock(KeyspaceHelper.class);
        this.exceptionFactory = Mockito.mock(RedisQuesExceptionFactory.class);
        this.redisRequestQuota = new Semaphore(1);
        this.configurationProvider = Mockito.mock(RedisquesConfigurationProvider.class);

        when(keyspaceHelper.getQueueStatisticQueueSizeSyncKey()).thenReturn("sync_key");
        QueueStatisticsCollector.CODECS_REGISTERED.set(false);
        queueStatisticsCollector = new QueueStatisticsCollector(redisService, keyspaceHelper, vertx,
                exceptionFactory, redisRequestQuota, 10, configurationProvider);
    }

    @Test
    public void testApproximateQueueSizeUpdate(TestContext context) {

        context.assertEquals(0L, queueStatisticsCollector.getApproximateQueueSize("test.queue.1"));
        context.assertEquals(0L, queueStatisticsCollector.getApproximateQueueSize("test.queue.2"));
        when(keyspaceHelper.getVerticleUid()).thenReturn("consumer-1");

        Set<String> aliveConsumer = new HashSet<>();
        aliveConsumer.add("consumer-1");
        aliveConsumer.add("consumer-2");
        aliveConsumer.add("consumer-3");

        Map<String, QueueProcessingState> myQueues = new HashMap<>();

        QueueProcessingState state1 = new QueueProcessingState(QueueState.READY, 0);
        state1.setQueueItemSize(42);
        QueueProcessingState state2 = new QueueProcessingState(QueueState.READY, 0);
        state2.setQueueItemSize(84);

        myQueues.put("test.queue.1", state1);
        myQueues.put("test.queue.2", state2);

        queueStatisticsCollector.updateApproximateQueueSize(aliveConsumer, myQueues);

        context.assertEquals(42L, queueStatisticsCollector.getApproximateQueueSize("test.queue.1"));
        context.assertEquals(84L, queueStatisticsCollector.getApproximateQueueSize("test.queue.2"));
    }
    /**
     * This test demonstrates a bug in getAllApproximateQueueSize():
     * When one consumer reports a queue with timestamp=0 (unknown), it unconditionally
     * overwrites data from another consumer that has a valid (newer) timestamp.
     *
     * The ts==0 branch should only be used as a fallback when no other data exists,
     * not as an override that defeats the timestamp-based merge logic.
     *
     * Note: The bug is order-dependent (ConcurrentHashMap iteration order).
     * We use multiple queues and consumers to increase probability of triggering it.
     */
    @Test
    public void testGetAllApproximateQueueSize_TimestampZeroShouldNotOverwriteNewerData(TestContext context) {
        Set<String> aliveConsumers = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            aliveConsumers.add("consumer-valid-" + i);
            aliveConsumers.add("consumer-zero-" + i);
        }

        for (int i = 0; i < 10; i++) {
            String queueName = "queue-" + i;

            when(keyspaceHelper.getVerticleUid()).thenReturn("consumer-valid-" + i);
            Map<String, QueueProcessingState> validQueues = new HashMap<>();
            QueueProcessingState validState = new QueueProcessingState(QueueState.READY, 1000 + i);
            validState.setQueueItemSize(100 + i);
            validQueues.put(queueName, validState);
            queueStatisticsCollector.updateApproximateQueueSize(aliveConsumers, validQueues);

            when(keyspaceHelper.getVerticleUid()).thenReturn("consumer-zero-" + i);
            Map<String, QueueProcessingState> zeroQueues = new HashMap<>();
            QueueProcessingState zeroState = new QueueProcessingState(QueueState.READY, 0);
            zeroState.setQueueItemSize(1 + i);
            zeroQueues.put(queueName, zeroState);
            queueStatisticsCollector.updateApproximateQueueSize(aliveConsumers, zeroQueues);
        }

        Map<String, Long> result = queueStatisticsCollector.getAllApproximateQueueSize();

        int bugCount = 0;
        for (int i = 0; i < 10; i++) {
            String queueName = "queue-" + i;
            long expected = 100 + i;
            long staleValue = 1 + i;
            Long actual = result.get(queueName);

            if (actual != null && actual == staleValue) {
                bugCount++;
            }
        }

        context.assertEquals(0, bugCount,
                "Found " + bugCount + " queues where ts=0 data overwrote valid timestamp data. " +
                        "The ts==0 case should not overwrite data with valid timestamps.");
    }

    /**
     * This test verifies that timestamp-based merge works correctly when both
     * consumers have valid (non-zero) timestamps.
     */
    @Test
    public void testGetAllApproximateQueueSize_NewerTimestampWins(TestContext context) {
        Set<String> aliveConsumers = new HashSet<>();
        aliveConsumers.add("consumer-A");
        aliveConsumers.add("consumer-B");

        // Consumer A reports queue "foo" with timestamp=1000, size=10
        when(keyspaceHelper.getVerticleUid()).thenReturn("consumer-A");
        Map<String, QueueProcessingState> consumerAQueues = new HashMap<>();
        QueueProcessingState stateA = new QueueProcessingState(QueueState.READY, 1000);
        stateA.setQueueItemSize(10);
        consumerAQueues.put("foo", stateA);
        queueStatisticsCollector.updateApproximateQueueSize(aliveConsumers, consumerAQueues);

        // Consumer B reports queue "foo" with timestamp=2000 (newer), size=20
        when(keyspaceHelper.getVerticleUid()).thenReturn("consumer-B");
        Map<String, QueueProcessingState> consumerBQueues = new HashMap<>();
        QueueProcessingState stateB = new QueueProcessingState(QueueState.READY, 2000);
        stateB.setQueueItemSize(20);
        consumerBQueues.put("foo", stateB);
        queueStatisticsCollector.updateApproximateQueueSize(aliveConsumers, consumerBQueues);

        // Get merged result
        Map<String, Long> result = queueStatisticsCollector.getAllApproximateQueueSize();

        // Expected: 20 (from consumer-B with the newer timestamp 2000)
        context.assertEquals(20L, result.get("foo"),
                "Queue size should be 20 (from consumer-B with newer timestamp=2000)");
    }

    /**
     * This test verifies that timestamp=0 data is used when no other data exists.
     */
    @Test
    public void testGetAllApproximateQueueSize_TimestampZeroUsedAsFallback(TestContext context) {
        Set<String> aliveConsumers = new HashSet<>();
        aliveConsumers.add("consumer-A");

        // Consumer A reports queue "foo" with timestamp=0, size=42
        when(keyspaceHelper.getVerticleUid()).thenReturn("consumer-A");
        Map<String, QueueProcessingState> consumerAQueues = new HashMap<>();
        QueueProcessingState stateA = new QueueProcessingState(QueueState.READY, 0);
        stateA.setQueueItemSize(42);
        consumerAQueues.put("foo", stateA);
        queueStatisticsCollector.updateApproximateQueueSize(aliveConsumers, consumerAQueues);

        // Get merged result
        Map<String, Long> result = queueStatisticsCollector.getAllApproximateQueueSize();

        // Expected: 42 (ts=0 should be used when it's the only data available)
        context.assertEquals(42L, result.get("foo"),
                "Queue size should be 42 when ts=0 is the only data available");
    }
}
