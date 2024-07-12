package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;
import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildAddQueueItemOperation;

/**
 * Tests for {@link AddQueueItemAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class AddQueueItemActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new AddQueueItemAction(vertx, redisProvider,
                "addr", "q-", "prefix-", "c-", "l-",
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testAddQueueItemWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildAddQueueItemOperation("queue2", "fooBar"));

        action.execute(message);

        verify(message, times(1)).reply(isA(ReplyException.class));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testAddQueueItem(TestContext context){
        when(message.body()).thenReturn(buildAddQueueItemOperation("queue2", "fooBar"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(redisAPI).rpush(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).rpush(anyList(), any());
        verify(message, times(1)).reply(eq(STATUS_OK));

    }

    @Test
    public void testAddQueueItemRPUSHFail(TestContext context){
        when(message.body()).thenReturn(buildAddQueueItemOperation("queue2", "fooBar"));

        doAnswer(invocation -> {
            var handler = createResponseHandler(invocation,1);
            handler.handle(Future.failedFuture("booom"));
            return null;
        }).when(redisAPI).rpush(anyList(), any());

        action.execute(message);

        verify(redisAPI, times(1)).rpush(anyList(), any());
        verify(message, times(1)).reply(isA(ReplyException.class));
    }
}
