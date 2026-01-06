package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.queue.QueueRegistryService;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildDeleteLockOperation;

/**
 * Tests for {@link DeleteLockAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class DeleteLockActionTest extends AbstractQueueActionTest {
    private QueueRegistryService registryService;
    @Before
    @Override
    public void setup() {
        super.setup();
        registryService = Mockito.mock(QueueRegistryService.class);
        action = new DeleteLockAction(vertx, registryService, redisService, keyspaceHelper,
                new ArrayList<>(), exceptionFactory, Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testDeleteLockWhenRedisIsNotReady(TestContext context) {
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        action.execute(message);

        verify(message, times(1)).reply(isA(ReplyException.class));
        verifyNoInteractions(redisAPI);
    }

    @Test
    public void testDeleteLock(TestContext context) {
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        when(redisAPI.exists(anyList())).thenReturn(Future.succeededFuture(SimpleStringType.create("0")));
        when(redisAPI.hdel(anyList())).thenReturn(Future.succeededFuture());

        action.execute(message);

        verify(redisAPI, times(1)).exists(anyList());
        verify(redisAPI, times(1)).hdel(anyList());
        verify(message, times(1)).reply(eq(STATUS_OK));
    }

    @Test
    public void testDeleteLockExistsFail(TestContext context) {
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        when(redisAPI.exists(anyList())).thenReturn(Future.failedFuture("booom"));
        when(redisAPI.hdel(anyList())).thenReturn(Future.succeededFuture());

        action.execute(message);

        verify(redisAPI, times(1)).exists(anyList());
        verify(redisAPI, times(1)).hdel(anyList());
        verify(message, times(1)).reply(eq(STATUS_OK));
    }

    @Test
    public void testDeleteLockHDELFail(TestContext context) {
        when(message.body()).thenReturn(buildDeleteLockOperation("testLock1"));

        when(redisAPI.exists(anyList())).thenReturn(Future.succeededFuture(SimpleStringType.create("0")));
        when(redisAPI.hdel(anyList())).thenReturn(Future.failedFuture("booom"));

        action.execute(message);

        verify(redisAPI, times(1)).exists(anyList());
        verify(redisAPI, times(1)).hdel(anyList());
        verify(message, times(1)).reply(isA(ReplyException.class));
    }
}
