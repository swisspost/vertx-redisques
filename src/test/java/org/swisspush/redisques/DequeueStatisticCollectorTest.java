package org.swisspush.redisques;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.redisques.util.DequeueStatistic;
import org.swisspush.redisques.util.DequeueStatisticCollector;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class DequeueStatisticCollectorTest {

    private Vertx vertx;
    private SharedData sharedData;
    private AsyncMap<String, DequeueStatistic> asyncMap;
    private DequeueStatisticCollector dequeueStatisticCollectorEnabled;
    private DequeueStatisticCollector dequeueStatisticCollectorDisabled;

    @Before
    public void setUp() {
        vertx = mock(Vertx.class);
        sharedData = mock(SharedData.class);
        asyncMap = mock(AsyncMap.class);

        doAnswer(invocation -> {
            io.vertx.core.Handler<io.vertx.core.AsyncResult<AsyncMap<String, DequeueStatistic>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(asyncMap));
            return null;
        }).when(sharedData).getAsyncMap(anyString(), any());

        when(vertx.sharedData()).thenReturn(sharedData);

        // Set up the enabled and disabled DequeueStatisticCollector
        dequeueStatisticCollectorEnabled = new DequeueStatisticCollector(vertx, true);
        dequeueStatisticCollectorDisabled = new DequeueStatisticCollector(vertx, false);
    }

    @Test
    public void testGetAllDequeueStatisticsEnabled(TestContext context) {
        // Mocking asyncMap.entries() to return a non-empty map
        Map<String, DequeueStatistic> dequeueStats = new HashMap<>();
        dequeueStats.put("queue1", new DequeueStatistic());
        when(asyncMap.entries()).thenReturn(Future.succeededFuture(dequeueStats));

        // Test for when dequeue statistics are enabled
        Async async = context.async();
        dequeueStatisticCollectorEnabled.getAllDequeueStatistics().onComplete(result -> {
            context.assertTrue(result.succeeded());
            context.assertEquals(1, result.result().size());
            async.complete();
        });

        // Verify that sharedData and asyncMap were used correctly
        verify(sharedData, times(1)).getAsyncMap(anyString(), any());
        verify(asyncMap, times(1)).entries();
    }

    @Test
    public void testGetAllDequeueStatisticsDisabled(TestContext context) {
        // Test for when dequeue statistics are disabled
        Async async = context.async();
        dequeueStatisticCollectorDisabled.getAllDequeueStatistics().onComplete(result -> {
            context.assertTrue(result.succeeded());
            context.assertTrue(result.result().isEmpty());
            async.complete();
        });

        // Verify that sharedData and asyncMap were NOT used
        verify(sharedData, never()).getAsyncMap(anyString(), any());
        verify(asyncMap, never()).entries();
    }
}
