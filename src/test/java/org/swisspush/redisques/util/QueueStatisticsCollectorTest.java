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

}
