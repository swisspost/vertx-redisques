package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class DefaultRedisProviderTest {

    private Vertx vertx;
    private Redis redis;
    private RedisConnection connection;
    private RedisquesConfigurationProvider configurationProvider;
    private DefaultRedisProvider provider;
    private MockedStatic<Redis> redisFactory;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> endHandler;
    private final List<Handler<Long>> timers = new ArrayList<>();

    @Before
    public void setUp() {
        vertx = mock(Vertx.class);
        redis = mock(Redis.class);
        connection = mock(RedisConnection.class);
        configurationProvider = mock(RedisquesConfigurationProvider.class);
        configure(-1);
        when(redis.connect()).thenReturn(Future.succeededFuture(connection));
        when(connection.send(any(Request.class))).thenReturn(Future.succeededFuture(SimpleStringType.create("PONG")));
        doAnswer(invocation -> {
            exceptionHandler = invocation.getArgument(0);
            return connection;
        }).when(connection).exceptionHandler(any());
        doAnswer(invocation -> {
            endHandler = invocation.getArgument(0);
            return connection;
        }).when(connection).endHandler(any());
        when(vertx.setTimer(anyLong(), any())).thenAnswer(invocation -> {
            timers.add(invocation.getArgument(1));
            return (long) timers.size();
        });
        redisFactory = mockStatic(Redis.class);
        redisFactory.when(() -> Redis.createClient(eq(vertx), any(RedisOptions.class))).thenReturn(redis);
        provider = new DefaultRedisProvider(vertx, configurationProvider);
    }

    @After
    public void tearDown() {
        redisFactory.close();
    }

    private void configure(int reconnectAttempts) {
        when(configurationProvider.configuration()).thenReturn(RedisquesConfiguration.with()
                .redisReconnectAttempts(reconnectAttempts)
                .redisReconnectDelaySec(1)
                .redisReadyCheckIntervalMs(0)
                .build());
    }

    private void fireTimer(int index) {
        timers.get(index).handle((long) index + 1);
    }

    @Test
    public void standaloneConnectionRemainsUsable() {
        Future<RedisAPI> api = provider.redis();
        assertTrue(api.succeeded());
        assertEquals("PONG", api.result().ping(List.of()).result().toString());
        assertSame(connection, provider.redisConnection().result());
        verify(connection, never()).close();
        verify(redis, times(1)).connect();
    }

    @Test
    public void concurrentCallersShareConnectionAttempt() {
        Promise<RedisConnection> pending = Promise.promise();
        when(redis.connect()).thenReturn(pending.future());
        Future<RedisAPI> first = provider.redis();
        Future<RedisAPI> second = provider.redis();
        Future<RedisConnection> rawConnection = provider.redisConnection();
        assertSame(first, second);
        assertFalse(first.isComplete());
        pending.complete(connection);
        assertTrue(first.succeeded());
        assertSame(connection, rawConnection.result());
        verify(redis, times(1)).connect();
    }

    @Test
    public void exceptionAndEndScheduleOnlyOneReconnect() {
        provider.redis();
        exceptionHandler.handle(new IllegalStateException("Redis restarted"));
        endHandler.handle(null);
        assertEquals(1, timers.size());
        fireTimer(0);
        assertSame(connection, provider.redisConnection().result());
        verify(redis, times(2)).connect();
        verify(vertx, times(1)).setTimer(eq(1000L), any());
    }

    @Test
    public void callersDuringReconnectWaitForNewConnection() {
        RedisAPI oldAPI = provider.redis().result();
        RedisConnection replacement = mock(RedisConnection.class);
        when(replacement.send(any(Request.class))).thenReturn(Future.succeededFuture(SimpleStringType.create("PONG")));
        Handler<Void> oldEndHandler = endHandler;
        endHandler.handle(null);
        Promise<RedisConnection> pending = Promise.promise();
        when(redis.connect()).thenReturn(pending.future());
        fireTimer(0);

        Future<RedisAPI> api = provider.redis();
        Future<RedisConnection> rawConnection = provider.redisConnection();
        assertFalse(api.isComplete());
        assertFalse(rawConnection.isComplete());
        pending.complete(replacement);

        assertTrue(api.succeeded());
        assertNotSame(oldAPI, api.result());
        assertSame(replacement, rawConnection.result());
        assertEquals("PONG", api.result().ping(List.of()).result().toString());
        oldEndHandler.handle(null);
        assertEquals(1, timers.size());
        verify(redis, times(2)).connect();
    }

    @Test
    public void foregroundReconnectCancelsPendingTimer() {
        provider.redis();
        endHandler.handle(null);
        assertTrue(provider.redis().succeeded());
        verify(vertx).cancelTimer(1L);
        fireTimer(0);
        verify(redis, times(2)).connect();
    }

    @Test
    public void failedReconnectUsesBackoffAndEventuallyRecovers() {
        provider.redis();
        endHandler.handle(null);
        when(redis.connect()).thenReturn(Future.failedFuture("Connection refused"));
        fireTimer(0);
        assertEquals(2, timers.size());
        verify(vertx).setTimer(eq(2000L), any());
        when(redis.connect()).thenReturn(Future.succeededFuture(connection));
        fireTimer(1);
        assertTrue(provider.redis().succeeded());
        verify(redis, times(3)).connect();
    }

    @Test
    public void failedInitialAttemptDoesNotBlockNextAttempt() {
        when(redis.connect()).thenReturn(Future.failedFuture("Connection refused"));
        assertTrue(provider.redis().failed());
        when(redis.connect()).thenReturn(Future.succeededFuture(connection));
        assertTrue(provider.redis().succeeded());
        verify(redis, times(2)).connect();
    }

    @Test
    public void delayedFailureCallbackDoesNotReconnectHealthyConnection() {
        provider.redis();
        endHandler.handle(null);
        Promise<RedisConnection> pending = Promise.promise();
        when(redis.connect()).thenReturn(pending.future());
        provider.redis().onFailure(ex -> {
            when(redis.connect()).thenReturn(Future.succeededFuture(connection));
            assertTrue(provider.redis().succeeded());
        });
        fireTimer(0);
        pending.fail("Connection refused");
        assertEquals(1, timers.size());
        verify(redis, times(3)).connect();
    }

    @Test
    public void synchronousFailureDoesNotLeavePendingPromise() {
        when(redis.connect()).thenThrow(new IllegalStateException("Client closed"));
        assertTrue(provider.redis().failed());
        doReturn(Future.succeededFuture(connection)).when(redis).connect();
        assertTrue(provider.redis().succeeded());
    }

    @Test
    public void reconnectCanBeDisabled() {
        configure(0);
        provider.redis();
        endHandler.handle(null);
        exceptionHandler.handle(new IllegalStateException("Redis stopped"));
        assertTrue(timers.isEmpty());
        verify(redis, times(1)).connect();
    }

    @Test
    public void configuredAttemptLimitIsRespected() {
        configure(1);
        provider.redis();
        endHandler.handle(null);
        when(redis.connect()).thenReturn(Future.failedFuture("Connection refused"));
        fireTimer(0);
        assertEquals(1, timers.size());
        verify(redis, times(2)).connect();
    }
}
