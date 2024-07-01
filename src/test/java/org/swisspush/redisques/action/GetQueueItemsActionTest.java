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
import static org.swisspush.redisques.util.RedisquesAPI.buildGetQueueItemsOperation;

/**
 * Tests for {@link GetQueueItemsAction} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class GetQueueItemsActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new GetQueueItemsAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testGetQueueItemsWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildGetQueueItemsOperation("q1", null));

        action.execute(message);

        verify(message, times(1)).fail(eq(0), eq("not ready"));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testLLENFail(TestContext context){
        when(message.body()).thenReturn(buildGetQueueItemsOperation("q1", null));
        when(redisAPI.llen(anyString())).thenReturn(Future.failedFuture("boooom"));

        action.execute(message);

        verify(redisAPI, times(1)).llen(anyString());
        verify(message, times(1)).fail(eq(0), eq("boooom"));
    }

    @Test
    public void testLLENInvalidResponseValue(TestContext context){
        when(message.body()).thenReturn(buildGetQueueItemsOperation("q1", null));
        when(redisAPI.llen(anyString())).thenReturn(Future.succeededFuture(SimpleStringType.create(null)));

        action.execute(message);

        verify(redisAPI, times(1)).llen(anyString());
        verify(message, times(1)).fail(eq(0), eq("Operation getQueueItems failed to extract queueItemCount"));
    }
}
