package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
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
import static org.swisspush.redisques.util.RedisquesAPI.buildReplaceQueueItemOperation;

/**
 * Tests for {@link ReplaceQueueItemAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class ReplaceQueueItemActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new ReplaceQueueItemAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testReplaceQueueItemWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildReplaceQueueItemOperation("q1", 0,"geronimo"));

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testReplaceQueueItem(TestContext context){
        when(message.body()).thenReturn(buildReplaceQueueItemOperation("q1", 0,"geronimo"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).lset(anyString(), anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), anyString(), anyString(), any());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\"}"))));
    }

    @Test
    public void testReplaceQueueItemWithLSETFail(TestContext context){
        when(message.body()).thenReturn(buildReplaceQueueItemOperation("q1", 0,"geronimo"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,3);
            handler.handle(Future.failedFuture("booom"));
            return null;
        }).when(redisAPI).lset(anyString(), anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), anyString(), anyString(), any());
        verify(message, times(1)).fail(eq(0), eq("booom"));
    }
}
