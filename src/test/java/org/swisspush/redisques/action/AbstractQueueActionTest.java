package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.junit.Before;
import org.mockito.Mockito;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.MemoryUsageProvider;
import org.swisspush.redisques.util.RedisProvider;

import java.util.Optional;

import static org.mockito.Mockito.*;

public abstract class AbstractQueueActionTest {

    protected RedisAPI redisAPI;
    protected RedisProvider redisProvider;
    protected RedisQuesExceptionFactory exceptionFactory;

    protected Vertx vertx;

    protected MemoryUsageProvider memoryUsageProvider;

    protected Message<JsonObject> message;
    protected AbstractQueueAction action;

    @Before
    public void setup() {
        redisAPI = Mockito.mock(RedisAPI.class);
        redisProvider = Mockito.mock(RedisProvider.class);
        exceptionFactory = Mockito.mock(RedisQuesExceptionFactory.class);
        when(redisProvider.redis()).thenReturn(Future.succeededFuture(redisAPI));

        memoryUsageProvider = Mockito.mock(MemoryUsageProvider.class);
        when(memoryUsageProvider.currentMemoryUsagePercentage()).thenReturn(Optional.empty());
        vertx = Vertx.vertx();

        message = Mockito.mock(Message.class);
    }

}
