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
import org.swisspush.redisques.util.RedisquesAPI;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Tests for {@link PutLockAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class PutLockActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new PutLockAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testPutLockWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildPutLockOperation("q1", "geronimo"));

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testPutLockWithoutRequestedByProperty(TestContext context){
        when(message.body()).thenReturn(buildOperation(RedisquesAPI.QueueOperation.putLock, new JsonObject().put(QUEUENAME, "q1")));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"message\":\"Property 'requestedBy' missing\"}"))));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testPutLockWithInvalidQueuenameProperty(TestContext context){
        when(message.body()).thenReturn(buildOperation(RedisquesAPI.QueueOperation.putLock, new JsonObject().put(REQUESTED_BY, "geronimo")));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"errorType\":\"bad input\",\"message\":\"Lock must be a string value\"}"))));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testPutLock(TestContext context){
        when(message.body()).thenReturn(buildPutLockOperation("q1", "geronimo"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).hmset(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).hmset(anyList(), any());
        verify(message, times(1)).reply(eq(STATUS_OK));
    }

    @Test
    public void testPutLockHMSETFail(TestContext context){
        when(message.body()).thenReturn(buildPutLockOperation("q1", "geronimo"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.failedFuture("booom"));
            return null;
        }).when(redisAPI).hmset(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).hmset(anyList(), any());
        verify(message, times(1)).fail(eq(0), eq("booom"));
    }
}
