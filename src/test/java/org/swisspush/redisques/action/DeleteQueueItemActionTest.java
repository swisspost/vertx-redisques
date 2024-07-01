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
import static org.swisspush.redisques.util.RedisquesAPI.buildDeleteQueueItemOperation;

/**
 * Tests for {@link DeleteQueueItemAction} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
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

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\"}"))));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testFailedLSET(TestContext context){
        when(redisAPI.lset(anyString(), anyString(), anyString())).thenReturn(Future.failedFuture("boooom"));
        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), eq("0"), eq("TO_DELETE"));
        verify(redisAPI, never()).lrem(anyString(), anyString(), anyString());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\"}"))));
    }

    @Test
    public void testFailedLREM(TestContext context){
        when(redisAPI.lset(anyString(), anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(redisAPI.lrem(anyString(), anyString(), anyString())).thenReturn(Future.failedFuture("boooom"));
        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), eq("0"), eq("TO_DELETE"));
        verify(redisAPI, times(1)).lrem(anyString(), eq("0"), eq("TO_DELETE"));
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\"}"))));
    }

    @Test
    public void testDeleteQueueItem(TestContext context){
        when(redisAPI.lset(anyString(), anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(redisAPI.lrem(anyString(), anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(message.body()).thenReturn(buildDeleteQueueItemOperation("queue1", 0));

        action.execute(message);

        verify(redisAPI, times(1)).lset(anyString(), eq("0"), eq("TO_DELETE"));
        verify(redisAPI, times(1)).lrem(anyString(), eq("0"), eq("TO_DELETE"));
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\"}"))));
    }
}
