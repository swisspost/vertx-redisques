package org.swisspush.redisques.queue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.QueueStatsService;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.MessageConsumerManager;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mock based tests of {@link QueueConsumerRunner} rebalance handoff behaviour which do not need a Redis server.
 */
@RunWith(VertxUnitRunner.class)
public class QueueConsumerRunnerAbandonTest {

    private Vertx vertx;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void abandonClaimedQueue_RenotifiesRestoredOwner(TestContext context) {
        RedisService redisService = mock(RedisService.class);
        KeyspaceHelper keyspaceHelper = mock(KeyspaceHelper.class);
        RedisquesConfigurationProvider configurationProvider = mock(RedisquesConfigurationProvider.class);
        RedisquesConfiguration configuration = mock(RedisquesConfiguration.class);

        when(configuration.getConsumerLockMultiplier()).thenReturn(2);
        when(configuration.getRefreshPeriod()).thenReturn(3);
        when(configuration.getMetricRefreshPeriod()).thenReturn(0);
        when(configurationProvider.configuration()).thenReturn(configuration);
        when(keyspaceHelper.getVerticleUid()).thenReturn("consumer-A");
        when(keyspaceHelper.getTrimRequestKey()).thenReturn("trim:");
        when(keyspaceHelper.getQueueRunningStateKey()).thenReturn("running-state");
        when(keyspaceHelper.getQueueRebalanceControlAddress()).thenReturn("rebalance-control");
        when(keyspaceHelper.getConsumersPrefix()).thenReturn("consumer:");
        when(keyspaceHelper.getVerticleNotifyConsumerKey()).thenReturn("notify-consumer");
        Response restoredResponse = mock(Response.class);
        when(restoredResponse.toInteger()).thenReturn(1);
        when(redisService.send(any())).thenReturn(Future.succeededFuture(restoredResponse));

        QueueConsumerRunner runner = new QueueConsumerRunner(vertx, redisService, mock(QueueMetrics.class), mock(QueueStatsService.class),
                keyspaceHelper, configurationProvider, RedisQuesExceptionFactory.newWastefulExceptionFactory(),
                mock(QueueStatisticsCollector.class), mock(QueueConfigurationProvider.class), new MessageConsumerManager(vertx));
        QueueProcessingState claimed = new QueueProcessingState(QueueState.READY, System.currentTimeMillis());
        claimed.setRebalancePending("consumer-B");
        runner.getMyQueues().put("q-abandon", claimed);

        AtomicBoolean notified = new AtomicBoolean();
        vertx.eventBus().consumer("notify-consumer", (Message<String> message) -> {
            context.assertEquals("q-abandon", message.body());
            notified.set(true);
            message.reply(null);
        });

        Async async = context.async();
        runner.abandonClaimedQueue("q-abandon", "consumer-B").onComplete(context.asyncAssertSuccess(restored -> {
            context.assertTrue(restored);
            context.assertFalse(runner.getMyQueues().containsKey("q-abandon"));
            context.assertTrue(notified.get());
            runner.unregisterConsumers(context.asyncAssertSuccess(v -> async.complete()));
        }));
    }
}
