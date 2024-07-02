package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
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
import static org.swisspush.redisques.util.RedisquesAPI.buildDeleteAllQueueItemsOperation;

/**
 * Tests for {@link DeleteAllQueueItemsAction} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class DeleteAllQueueItemsActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new DeleteAllQueueItemsAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testDeleteAllQueueItemsNoUnlock(TestContext context){
        when(message.body()).thenReturn(buildDeleteAllQueueItemsOperation("q1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture(SimpleStringType.create("1")));
            return null;
        }).when(redisAPI).del(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).del(anyList(), any());
        verify(redisAPI, never()).hdel(anyList());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"value\":1}"))));
    }

    @Test
    public void testDeleteAllQueueItemsWithUnlock(TestContext context){
        when(message.body()).thenReturn(buildDeleteAllQueueItemsOperation("q1", true));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture(SimpleStringType.create("1")));
            return null;
        }).when(redisAPI).del(anyList(), any());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture(SimpleStringType.create("1")));
            return null;
        }).when(redisAPI).hdel(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).del(anyList(), any());
        verify(redisAPI, times(1)).hdel(anyList(), any());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"value\":1}"))));
    }

    @Test
    public void testDeleteAllQueueItemsWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildDeleteAllQueueItemsOperation("q1"));

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testRedisApiDELFail(TestContext context){
        when(message.body()).thenReturn(buildDeleteAllQueueItemsOperation("q1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.failedFuture("boooom"));
            return null;
        }).when(redisAPI).del(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).del(anyList(), any());
        verify(message, times(1)).fail(eq(0), eq("boooom"));
    }

    @Test
    public void testRedisApiUnlockFail(TestContext context){
        when(message.body()).thenReturn(buildDeleteAllQueueItemsOperation("q1", true));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture(SimpleStringType.create("1")));
            return null;
        }).when(redisAPI).del(anyList(), any());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.failedFuture("boooom"));
            return null;
        }).when(redisAPI).hdel(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).del(anyList(), any());
        verify(redisAPI, times(1)).hdel(anyList(), any());
        verify(message, times(1)).fail(eq(0), eq("boooom"));
    }
}
