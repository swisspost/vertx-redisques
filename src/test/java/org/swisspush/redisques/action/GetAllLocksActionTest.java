package org.swisspush.redisques.action;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.impl.future.SucceededFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.MultiType;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildGetAllLocksOperation;

/**
 * Tests for {@link GetAllLocksAction} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class GetAllLocksActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new GetAllLocksAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testGetAllLocksWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildGetAllLocksOperation());

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testGetAllLocksInvalidFilter(TestContext context){
        when(message.body()).thenReturn(buildGetAllLocksOperation("xyz(.*"));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(
                Buffer.buffer("{\"status\":\"error\",\"errorType\":\"bad input\",\"message\":\"Error while compile" +
                        " regex pattern. Cause: Unclosed group near index 6\\nxyz(.*\"}"))));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testGetAllLocksHKEYSFail(TestContext context){
        when(message.body()).thenReturn(buildGetAllLocksOperation());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation, 1);
            handler.handle(new FailedFuture("booom"));
            return null;
        }).when(redisAPI).hkeys(anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).hkeys(anyString(), any());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\"}"))));
    }

    @Test
    public void testGetAllLocks(TestContext context){
        when(message.body()).thenReturn(buildGetAllLocksOperation());

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation, 1);
            MultiType response = MultiType.create(2, false);
            response.add(SimpleStringType.create("foo"));
            response.add(SimpleStringType.create("bar"));
            handler.handle(new SucceededFuture<>(response));
            return null;
        }).when(redisAPI).hkeys(anyString(), any());

        action.execute(message);

        verify(redisAPI, times(1)).hkeys(anyString(), any());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"value\":{\"locks\":[\"foo\",\"bar\"]}}"))));
    }
}
