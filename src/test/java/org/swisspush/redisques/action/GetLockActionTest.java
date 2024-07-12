package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
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
import static org.swisspush.redisques.util.RedisquesAPI.buildGetLockOperation;

/**
 * Tests for {@link GetLockAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class GetLockActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new GetLockAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testGetLockWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildGetLockOperation("q1"));

        action.execute(message);

        verify(message, times(1)).reply(isA(ReplyException.class));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testGetLock(TestContext context){
        when(message.body()).thenReturn(buildGetLockOperation("q1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,2);
            handler.handle(Future.succeededFuture(SimpleStringType.create(new JsonObject().put("requestedBy", "UNKNOWN").put("timestamp", 1719931433522L).encode())));
            return null;
        }).when(redisAPI).hget(anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).hget(anyString(), anyString(), any());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\"," +
                "\"value\":\"{\\\"requestedBy\\\":\\\"UNKNOWN\\\",\\\"timestamp\\\":1719931433522}\"}"))));
    }

    @Test
    public void testGetLockNoSuchLock(TestContext context){
        when(message.body()).thenReturn(buildGetLockOperation("q1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).hget(anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).hget(anyString(), anyString(), any());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"No such lock\"}"))));
    }

    @Test
    public void testGetLockHGETFail(TestContext context){
        when(message.body()).thenReturn(buildGetLockOperation("q1"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,2);
            handler.handle(Future.failedFuture("booom"));
            return null;
        }).when(redisAPI).hget(anyString(), anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).hget(anyString(), anyString(), any());
        //verify(message, times(1)).fail(eq(0), eq("booom"));
        verify(message, times(1)).reply(isA(ReplyException.class));
    }
}
