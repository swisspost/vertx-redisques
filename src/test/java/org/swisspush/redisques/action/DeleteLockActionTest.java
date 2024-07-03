package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildDeleteLockOperation;

/**
 * Tests for {@link DeleteLockAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class DeleteLockActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new DeleteLockAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testDeleteLockWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testDeleteLock(TestContext context){
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture(SimpleStringType.create("0")));
            return null;
        }).when(redisAPI).exists(anyList(), any());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).hdel(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).exists(anyList(), any());
        verify(redisAPI, times(1)).hdel(anyList(), any());
        verify(message, times(1)).reply(eq(STATUS_OK));
    }

    @Test
    public void testDeleteLockExistsFail(TestContext context){
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.failedFuture("booom"));
            return null;
        }).when(redisAPI).exists(anyList(), any());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).hdel(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).exists(anyList(), any());
        verify(redisAPI, times(1)).hdel(anyList(), any());
        verify(message, times(1)).reply(eq(STATUS_OK));
    }

    @Test
    public void testDeleteLockHDELFail(TestContext context){
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture(SimpleStringType.create("0")));
            return null;
        }).when(redisAPI).exists(anyList(), any());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.failedFuture("booom"));
            return null;
        }).when(redisAPI).hdel(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).exists(anyList(), any());
        verify(redisAPI, times(1)).hdel(anyList(), any());
        verify(message, times(1)).fail(eq(0), eq("booom"));
    }
}
