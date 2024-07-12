package org.swisspush.redisques.action;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.MemoryUsageProvider;
import org.swisspush.redisques.util.RedisProvider;

import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;

public abstract class AbstractQueueActionTest {

    protected RedisAPI redisAPI;
    protected RedisProvider redisProvider;
    protected RedisQuesExceptionFactory exceptionFactory;

    protected Vertx vertx;

    protected MemoryUsageProvider memoryUsageProvider;

    protected Message<JsonObject> message;
    protected AbstractQueueAction action;

    protected static final JsonObject STATUS_OK = new JsonObject(Buffer.buffer("{\"status\":\"ok\"}"));
    protected static final JsonObject STATUS_ERROR = new JsonObject(Buffer.buffer("{\"status\":\"error\"}"));

    @Before
    public void setup() {
        redisAPI = Mockito.mock(RedisAPI.class);
        redisProvider = Mockito.mock(RedisProvider.class);
        exceptionFactory = newWastefulExceptionFactory();
        when(redisProvider.redis()).thenReturn(Future.succeededFuture(redisAPI));

        memoryUsageProvider = Mockito.mock(MemoryUsageProvider.class);
        when(memoryUsageProvider.currentMemoryUsagePercentage()).thenReturn(Optional.empty());
        vertx = Vertx.vertx();

        message = Mockito.mock(Message.class);
    }

    protected Handler<AsyncResult<Response>> createResponseHandler(InvocationOnMock invocation, int handlerIndex) {
        return (Handler<AsyncResult<Response>>) invocation.getArguments()[handlerIndex];
    }
}
