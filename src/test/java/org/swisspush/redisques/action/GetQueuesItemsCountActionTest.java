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
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildGetQueuesItemsCountOperation;

/**
 * Tests for {@link GetQueuesItemsCountAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class GetQueuesItemsCountActionTest extends AbstractQueueActionTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        action = new GetQueuesItemsCountAction(vertx, redisService, keyspaceHelper,
                new ArrayList<>(), Mockito.mock(RedisQuesExceptionFactory.class), Mockito.mock(Semaphore.class),
                Mockito.mock(QueueStatisticsCollector.class), Mockito.mock(Logger.class));
    }

    @Test
    public void testGetQueuesItemsCountWhenRedisIsNotReady(TestContext context){
        when(redisProvider.redis()).thenReturn(Future.failedFuture("not ready"));
        when(message.body()).thenReturn(buildGetQueuesItemsCountOperation(null));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(Buffer.buffer("{\"status\":\"error\"}"))));
        verifyNoInteractions(redisAPI);
    }
}
