package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
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
import org.swisspush.redisques.util.RedisquesAPI;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildBulkDeleteQueuesOperation;
import static org.swisspush.redisques.util.RedisquesAPI.buildOperation;

/**
 * Tests for {@link BulkDeleteQueuesAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class BulkDeleteQueuesActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new BulkDeleteQueuesAction(vertx, redisService, keyspaceHelper,
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testBulkDeleteQueuesWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildBulkDeleteQueuesOperation(new JsonArray().add("q1").add("q3")));

        action.execute(message);

        verify(message, times(1)).reply(isA(ReplyException.class));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testBulkDeleteQueues(TestContext context){
        when(message.body()).thenReturn(buildBulkDeleteQueuesOperation(new JsonArray().add("q1").add("q3")));

        when(redisAPI.del(anyList())).thenReturn(Future.succeededFuture(SimpleStringType.create("2")));
        action.execute(message);

        verify(redisAPI, times(1)).del(anyList());
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"value\":2}"))));
    }

    @Test
    public void testBulkDeleteQueuesDELFail(TestContext context){
        when(message.body()).thenReturn(buildBulkDeleteQueuesOperation(new JsonArray().add("q1").add("q3")));

        when(redisAPI.del(anyList())).thenReturn(Future.failedFuture("booom"));
        action.execute(message);

        verify(redisAPI, times(1)).del(anyList());
        verify(message, times(1)).reply(isA(ReplyException.class));
    }

    @Test
    public void testBulkDeleteQueuesNoQueuesProvided(TestContext context){
        when(message.body()).thenReturn(buildOperation(RedisquesAPI.QueueOperation.bulkDeleteQueues, new JsonObject().put("SomeProperty", "foobar")));

        action.execute(message);

        verifyNoInteractions(redisAPI);
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"message\":\"No queues to delete provided\"}"))));
    }

    @Test
    public void testBulkDeleteQueuesEmptyQueuesProvided(TestContext context){
        when(message.body()).thenReturn(buildBulkDeleteQueuesOperation(new JsonArray()));

        action.execute(message);

        verifyNoInteractions(redisAPI);
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"ok\",\"value\":0}"))));
    }

    @Test
    public void testBulkDeleteQueuesInvalidQueuesEntries(TestContext context){
        when(message.body()).thenReturn(buildBulkDeleteQueuesOperation(new JsonArray().add(new JsonObject()).add(new JsonArray())));

        action.execute(message);

        verifyNoInteractions(redisAPI);
        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\",\"errorType\":\"bad input\",\"message\":\"Queues must be string values\"}"))));
    }
}
