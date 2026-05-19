package org.swisspush.redisques.queue;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class RedisServiceTest {
    private Vertx vertx;
    private RedisProvider redisProvider;
    private RedisquesConfigurationProvider configurationProvider;
    private RedisquesConfiguration configuration;
    private RedisConnection connection;
    private RedisService redisService;

    private int mockSendIndex;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();

        redisProvider = mock(RedisProvider.class);
        configurationProvider = mock(RedisquesConfigurationProvider.class);
        configuration = mock(RedisquesConfiguration.class);
        connection = mock(RedisConnection.class);

        when(configurationProvider.configuration()).thenReturn(configuration);
        when(redisProvider.redisConnection()).thenReturn(Future.succeededFuture(connection));

        redisService = new RedisService(vertx, redisProvider, configurationProvider);

        RedisService.isClusterMode.set(false);
        mockSendIndex = 0;
    }

    @Test
    public void testScanClusterShouldReturnKeyValueMap_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        when(configuration.getRedisClientType()).thenReturn(RedisClientType.STANDALONE);

        mockSend(scanResponse("0", "key:{same}:1", "key:{same}:2"));

        Future<List<Response>> future = Future.succeededFuture(Arrays.asList(stringResponse("value1"), stringResponse("value2")));
        when(connection.batch(anyList())).thenReturn(future);

        redisService.scanCluster("key:*").onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(2, result.size());
            ctx.assertEquals("value1", result.get("key:{same}:1"));
            ctx.assertEquals("value2", result.get("key:{same}:2"));
            async.complete();
        }));
    }

    @Test
    public void testScanClusterShouldContinueWhenFirstScanPageIsEmpty_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        when(configuration.getRedisClientType()).thenReturn(RedisClientType.STANDALONE);

        mockSend(scanResponse("42"), scanResponse("0", "key:{1}"));

        Future<List<Response>> future = Future.succeededFuture(Arrays.asList(stringResponse("value1")));
        when(connection.batch(anyList())).thenReturn(future);

        redisService.scanCluster("key:*").onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(1, result.size());
            ctx.assertEquals("value1", result.get("key:{1}"));
            async.complete();
        }));
    }

    @Test
    public void testScanClusterShouldScanMultiplePages_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        when(configuration.getRedisClientType()).thenReturn(RedisClientType.STANDALONE);

        mockSend(scanResponse("42", "key:{a}:1"), scanResponse("0", "key:{b}:1"));

        Future<List<Response>> future1 = Future.succeededFuture(Collections.singletonList(stringResponse("value1")));
        Future<List<Response>> future2 = Future.succeededFuture(Collections.singletonList(stringResponse("value2")));
        when(connection.batch(anyList())).thenReturn(future1).thenReturn(future2);

        redisService.scanCluster("key:*").onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(2, result.size());

            ctx.assertEquals("value1", result.get("key:{a}:1"));
            ctx.assertEquals("value2", result.get("key:{b}:1"));

            async.complete();
        }));
    }

    @Test
    public void testScanClusterShouldFailWhenScanFails_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        when(configuration.getRedisClientType()).thenReturn(RedisClientType.STANDALONE);

        RuntimeException failure = new RuntimeException("scan failed");
        mockSendFailure(failure);

        redisService.scanCluster("key:*").onComplete(ctx.asyncAssertFailure(err -> {
            ctx.assertEquals(failure, err);
            async.complete();
        }));
    }

    @Test
    public void testScanClusterShouldFailWhenBatchGetFails_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        when(configuration.getRedisClientType()).thenReturn(RedisClientType.STANDALONE);

        RuntimeException failure = new RuntimeException("batch failed");

        mockSend(scanResponse("0", "key:{1}"));
        when(connection.batch(anyList())).thenReturn(Future.failedFuture(failure));

        redisService.scanCluster("key:*").onComplete(ctx.asyncAssertFailure(err -> {
            ctx.assertEquals(failure, err);
            async.complete();
        }));
    }

    @Test
    public void testScanClusterShouldFailWhenClusterSlotsCommandFails_ClusterMode(TestContext ctx) {
        Async async = ctx.async();

        when(configuration.getRedisClientType()).thenReturn(RedisClientType.CLUSTER);

        RuntimeException failure = new RuntimeException("cluster slots failed");
        when(connection.send(any(Request.class))).thenReturn(Future.failedFuture(failure));

        redisService.scanCluster("key:*").onComplete(ctx.asyncAssertFailure(err -> {
            ctx.assertEquals(failure, err);
            async.complete();
        }));
    }

    @Test
    public void testClusterSafeBatchShouldReturnEmptyListForEmptyKeys_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        redisService.clusterSafeBatch(Command.GET, Collections.emptyList(), Collections.emptyList()).onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertTrue(result.isEmpty());
            async.complete();
        }));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClusterSafeBatchShouldRejectTooManyKeys() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < RedisService.MAX_COMMANDS_IN_BATCH + 1; i++) {
            keys.add("key:{" + i + "}");
        }

        redisService.clusterSafeBatch(Command.GET, keys, Collections.emptyList());
    }

    @Test
    public void testClusterSafeBatchShouldKeepOriginalOrder_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        RedisService.isClusterMode.set(false);

        Future<List<Response>> future = Future.succeededFuture(Arrays.asList(stringResponse("value1"), stringResponse("value2"), stringResponse("value3")));
        when(connection.batch(anyList())).thenReturn(future);

        redisService.clusterSafeBatch(Command.GET, Arrays.asList("key:{1}", "key:{2}", "key:{3}"), Collections.emptyList()).onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(3, result.size());
            ctx.assertEquals("value1", result.get(0).toString());
            ctx.assertEquals("value2", result.get(1).toString());
            ctx.assertEquals("value3", result.get(2).toString());
            async.complete();
        }));
    }

    @Test
    public void testClusterSafeBatchShouldFailWhenBatchFails_StandaloneMode(TestContext ctx) {
        Async async = ctx.async();

        RuntimeException failure = new RuntimeException("batch failed");

        when(connection.batch(anyList())).thenReturn(Future.failedFuture(failure));

        redisService.clusterSafeBatch(Command.GET, Collections.singletonList("key:{1}"), Collections.emptyList()).onComplete(ctx.asyncAssertFailure(err -> {
            ctx.assertEquals(failure, err);
            async.complete();
        }));
    }

    @Test
    public void testClusterSafeBatchShouldGroupSameSlotKeysIntoOneBatch_ClusterMode(TestContext ctx) {
        Async async = ctx.async();

        RedisService.isClusterMode.set(true);

        Future<List<Response>> future = Future.succeededFuture(Arrays.asList(stringResponse("value1"), stringResponse("value2")));
        when(connection.batch(anyList())).thenReturn(future);

        redisService.clusterSafeBatch(Command.GET, Arrays.asList("key:{same}:1", "key:{same}:2"), Collections.emptyList()).onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(2, result.size());
            ctx.assertEquals("value1", result.get(0).toString());
            ctx.assertEquals("value2", result.get(1).toString());

            verify(connection, times(1)).batch(anyList());
            async.complete();
        }));
    }

    @Test
    public void testClusterSafeBatchShouldSplitDifferentSlotKeysIntoDifferentBatches_ClusterMode(TestContext ctx) {
        Async async = ctx.async();

        RedisService.isClusterMode.set(true);

        Future<List<Response>> future1 = Future.succeededFuture(Collections.singletonList(stringResponse("value1")));
        Future<List<Response>> future2 = Future.succeededFuture(Collections.singletonList(stringResponse("value2")));
        when(connection.batch(anyList())).thenReturn(future1).thenReturn(future2);

        redisService.clusterSafeBatch(Command.GET, Arrays.asList("key:{slot1}", "key:{slot2}"), Collections.emptyList()).onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(2, result.size());
            ctx.assertEquals("value1", result.get(0).toString());
            ctx.assertEquals("value2", result.get(1).toString());

            verify(connection, times(2)).batch(anyList());
            async.complete();
        }));
    }

    @Test
    public void testClusterSafeBatchShouldKeepOriginalOrder_ClusterMode(TestContext ctx) {
        Async async = ctx.async();

        RedisService.isClusterMode.set(true);

        Future<List<Response>> future1 = Future.succeededFuture(Collections.singletonList(stringResponse("value1")));
        Future<List<Response>> future2 = Future.succeededFuture(Collections.singletonList(stringResponse("value2")));
        Future<List<Response>> future3 = Future.succeededFuture(Collections.singletonList(stringResponse("value3")));
        when(connection.batch(anyList())).thenReturn(future1).thenReturn(future2).thenReturn(future3);

        redisService.clusterSafeBatch(Command.GET, Arrays.asList("key:{slot1}", "key:{slot2}", "key:{slot3}"), Collections.emptyList()).onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(3, result.size());
            ctx.assertEquals("value1", result.get(0).toString());
            ctx.assertEquals("value2", result.get(1).toString());
            ctx.assertEquals("value3", result.get(2).toString());

            verify(connection, times(3)).batch(anyList());
            async.complete();
        }));
    }

    @Test
    public void testClusterSafeBatchShouldPassAdditionalArgs_ClusterMode(TestContext ctx) {
        Async async = ctx.async();

        RedisService.isClusterMode.set(true);
        Future<List<Response>> futureOk = Future.succeededFuture(Collections.singletonList(stringResponse("OK")));
        when(connection.batch(anyList())).thenReturn(futureOk);

        redisService.clusterSafeBatch(Command.SET, Collections.singletonList("key:{1}"), Arrays.asList("value1", "PX", "1000")).onComplete(ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(1, result.size());
            ctx.assertEquals("OK", result.get(0).toString());

            verify(connection, times(1)).batch(anyList());
            async.complete();
        }));
    }

    private void mockSend(Response... responses) {
        doAnswer(invocation -> {
            Handler<AsyncResult<Response>> handler = invocation.getArgument(1);

            int index = mockSendIndex++;
            handler.handle(Future.succeededFuture(responses[index]));

            return connection;
        }).when(connection).send(any(Request.class), any(Handler.class));
    }

    private void mockSendFailure(Throwable failure) {
        doAnswer(invocation -> {
            Handler<AsyncResult<Response>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(failure));
            return connection;
        }).when(connection).send(any(Request.class), any(Handler.class));
    }

    private Response scanResponse(String cursor, String... keys) {
        Response response = mock(Response.class);
        Response cursorResponse = stringResponse(cursor);
        Response keysResponse = responseList(keys);

        when(response.get(0)).thenReturn(cursorResponse);
        when(response.get(1)).thenReturn(keysResponse);

        return response;
    }

    private Response responseList(String... values) {
        Response response = mock(Response.class);

        List<Response> list = new ArrayList<>();
        for (String value : values) {
            list.add(stringResponse(value));
        }

        when(response.iterator()).thenReturn(list.iterator());
        when(response.stream()).thenReturn(list.stream());
        when(response.size()).thenReturn(list.size());

        for (int i = 0; i < list.size(); i++) {
            Response item = list.get(i);
            when(response.get(i)).thenReturn(item);
        }

        return response;
    }

    private Response stringResponse(String value) {
        Response response = mock(Response.class);
        when(response.toString()).thenReturn(value);
        return response;
    }
}