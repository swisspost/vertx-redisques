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
import static org.swisspush.redisques.util.RedisquesAPI.buildGetQueueItemOperation;

/**
 * Tests for {@link GetQueueItemAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class GetQueueItemActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new GetQueueItemAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testGetQueueItemWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildGetQueueItemOperation("q1", 0));

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testGetQueueItem(TestContext context){
        when(message.body()).thenReturn(buildGetQueueItemOperation("q1", 0));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,2);
            handler.handle(Future.succeededFuture(SimpleStringType.create(new JsonObject().put("foo", "bar").encode())));
            return null;
        }).when(redisAPI).lindex(anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).lindex(anyString(), anyString(), any());
        verify(message, times(1)).reply(eq(new JsonObject(
                Buffer.buffer("{\"status\":\"ok\",\"value\":\"{\\\"foo\\\":\\\"bar\\\"}\"}"))));
    }

    @Test
    public void testGetQueueItemNotExistingIndex(TestContext context){
        when(message.body()).thenReturn(buildGetQueueItemOperation("q1", 0));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).lindex(anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).lindex(anyString(), anyString(), any());
        verify(message, times(1)).reply(eq(STATUS_ERROR));
    }

    @Test
    public void testGetQueueItemLINDEXFail(TestContext context){
        when(message.body()).thenReturn(buildGetQueueItemOperation("q1", 0));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,2);
            handler.handle(Future.failedFuture("booom"));
            return null;
        }).when(redisAPI).lindex(anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).lindex(anyString(), anyString(), any());
        verify(message, times(1)).fail(eq(0), eq("booom"));
    }
}
