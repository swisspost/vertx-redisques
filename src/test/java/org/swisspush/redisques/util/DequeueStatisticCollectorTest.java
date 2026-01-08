package org.swisspush.redisques.util;

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
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class DequeueStatisticCollectorTest {

    private Vertx vertx;
    private SharedData sharedData;
    private RedisService redisService;
    private KeyspaceHelper keyspaceHelper;
    private DequeueStatisticCollector dequeueStatisticCollectorEnabled;
    private DequeueStatisticCollector dequeueStatisticCollectorDisabled;

//    @Before
//    public void setUp() {
//        vertx = mock(Vertx.class);
//        sharedData = mock(SharedData.class);
//        redisService = mock(RedisService.class);
//        keyspaceHelper = mock(KeyspaceHelper.class);
//
//        // Mock sharedData.getAsyncMap to return asyncMap
//        doAnswer(invocation -> {
//            io.vertx.core.Handler<io.vertx.core.AsyncResult<AsyncMap<String, DequeueStatistic>>> handler = invocation.getArgument(1);
//            handler.handle(Future.succeededFuture(asyncMap));
//            return null;
//        }).when(sharedData).getAsyncMap(anyString(), any());
//
//        when(vertx.sharedData()).thenReturn(sharedData);
//
//        // Initialize DequeueStatisticCollector with enabled/disabled stats collection
//        dequeueStatisticCollectorEnabled = new DequeueStatisticCollector(true, redisService, keyspaceHelper);
//        dequeueStatisticCollectorDisabled = new DequeueStatisticCollector(false, redisService, keyspaceHelper);
//    }
//
//    @Test
//    public void testGetAllDequeueStatisticsEnabled(TestContext context) {
//        // Mock asyncMap.entries() to return a non-empty map
//        Map<String, DequeueStatistic> dequeueStats = new HashMap<>();
//        dequeueStats.put("queue1", new DequeueStatistic());
//        when(asyncMap.entries()).thenReturn(Future.succeededFuture(dequeueStats));
//
//        // Test when dequeue statistics are enabled
//        Async async = context.async();
//        dequeueStatisticCollectorEnabled.getAllDequeueStatistics().onComplete(result -> {
//            context.assertTrue(result.succeeded());
//            context.assertEquals(1, result.result().size());
//            async.complete();
//        });
//
//        // Verify that sharedData and asyncMap were used correctly
//        verify(sharedData, times(1)).getAsyncMap(anyString(), any());
//        verify(asyncMap, times(1)).entries();
//    }
//
//    @Test
//    public void testGetAllDequeueStatisticsDisabled(TestContext context) {
//        // Test when dequeue statistics are disabled
//        Async async = context.async();
//        dequeueStatisticCollectorDisabled.getAllDequeueStatistics().onComplete(result -> {
//            context.assertTrue(result.succeeded());
//            context.assertTrue(result.result().isEmpty());
//            async.complete();
//        });
//
//        // Verify that sharedData and asyncMap were NOT used
//        verifyNoInteractions(sharedData);
//        verifyNoInteractions(asyncMap);
//    }
//
//    @Test
//    public void testGetAllDequeueStatisticsAsyncMapFailure(TestContext context) {
//        // Simulate failure in sharedData.getAsyncMap
//        doAnswer(invocation -> {
//            io.vertx.core.Handler<io.vertx.core.AsyncResult<AsyncMap<String, DequeueStatistic>>> handler = invocation.getArgument(1);
//            handler.handle(Future.failedFuture(new RuntimeException("Failed to retrieve async map")));
//            return null;
//        }).when(sharedData).getAsyncMap(anyString(), any());
//
//        // Test when asyncMap retrieval fails
//        Async async = context.async();
//        dequeueStatisticCollectorEnabled.getAllDequeueStatistics().onComplete(result -> {
//            context.assertTrue(result.failed());
//            context.assertEquals("Failed to retrieve async map", result.cause().getMessage());
//            async.complete();
//        });
//
//        // Verify that sharedData.getAsyncMap was used, but asyncMap.entries() was not
//        verify(sharedData, times(1)).getAsyncMap(anyString(), any());
//        verifyNoInteractions(asyncMap);
//    }
//
//    @Test
//    public void testGetAllDequeueStatisticsEntriesFailure(TestContext context) {
//        // Simulate success in sharedData.getAsyncMap, but failure in asyncMap.entries
//        doAnswer(invocation -> {
//            io.vertx.core.Handler<io.vertx.core.AsyncResult<AsyncMap<String, DequeueStatistic>>> handler = invocation.getArgument(1);
//            handler.handle(Future.succeededFuture(asyncMap));
//            return null;
//        }).when(sharedData).getAsyncMap(anyString(), any());
//
//        when(asyncMap.entries()).thenReturn(Future.failedFuture(new RuntimeException("Failed to retrieve entries")));
//
//        // Test when asyncMap.entries fails
//        Async async = context.async();
//        dequeueStatisticCollectorEnabled.getAllDequeueStatistics().onComplete(result -> {
//            context.assertTrue(result.failed());
//            context.assertEquals("Failed to retrieve entries", result.cause().getMessage());
//            async.complete();
//        });
//
//        // Verify that sharedData.getAsyncMap and asyncMap.entries were used correctly
//        verify(sharedData, times(1)).getAsyncMap(anyString(), any());
//        verify(asyncMap, times(1)).entries();
//    }
}
