package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildDeleteQueueItemOperation;

/**
 * Tests for {@link DeleteQueueItemAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class DeleteQueueItemActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new DeleteQueueItemAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testDeleteQueueItemWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testFailedLSET(TestContext context){
        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.failedFuture("boooom"));
            return null;
        }).when(redisAPI).lset(anyString(), anyString(), anyString(), any());

        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), eq("0"), eq("TO_DELETE"), any());
        verify(redisAPI, never()).lrem(anyString(), anyString(), anyString());
        verify(message, times(1)).fail(eq(0), eq("boooom"));
    }

    @Test
    public void testFailedLSETNoSuchKey(TestContext context){
        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.failedFuture("ERR no such key"));
            return null;
        }).when(redisAPI).lset(anyString(), anyString(), anyString(), any());

        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), eq("0"), eq("TO_DELETE"), any());
        verify(redisAPI, never()).lrem(anyString(), anyString(), anyString());
        verify(message, times(1)).reply(eq(STATUS_ERROR));
    }

    @Test
    public void testFailedLREM(TestContext context){
        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).lset(anyString(), anyString(), anyString(), any());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.failedFuture("boooom"));
            return null;
        }).when(redisAPI).lrem(anyString(), anyString(), anyString(), any());

        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), eq("0"), eq("TO_DELETE"), any());
        verify(redisAPI, times(1)).lrem(anyString(), eq("0"), eq("TO_DELETE"), any());
        verify(message, times(1)).fail(eq(0), eq("boooom"));
    }

    @Test
    public void testDeleteQueueItem(TestContext context){

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).lset(anyString(), anyString(), anyString(), any());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).lrem(anyString(), anyString(), anyString(), any());

        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), eq("0"), eq("TO_DELETE"), any());
        verify(redisAPI, times(1)).lrem(anyString(), eq("0"), eq("TO_DELETE"), any());
        verify(message, times(1)).reply(eq(STATUS_OK));
    }
}
