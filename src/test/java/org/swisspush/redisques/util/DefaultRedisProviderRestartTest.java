package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DefaultRedisProviderRestartTest {

    private Vertx vertx;
    private NetServer server;
    private final AtomicReference<NetSocket> socket = new AtomicReference<>();
    private final AtomicBoolean respondToPing = new AtomicBoolean(true);
    private final AtomicBoolean pingReceived = new AtomicBoolean();

    @Before
    public void setUp() throws Exception {
        vertx = Vertx.vertx();
        server = startServer(0);
    }

    @After
    public void tearDown() throws Exception {
        get(vertx.close());
    }

    @Test(timeout = 15000)
    public void recoversAfterServerRestartAndFailsOutstandingCommands() throws Exception {
        RedisquesConfigurationProvider configurationProvider = mock(RedisquesConfigurationProvider.class);
        when(configurationProvider.configuration()).thenReturn(RedisquesConfiguration.with()
                .redisHost("127.0.0.1")
                .redisPort(server.actualPort())
                .redisReconnectAttempts(-1)
                .redisReconnectDelaySec(1)
                .redisPoolRecycleTimeoutMs(-1)
                .redisReadyCheckIntervalMs(0)
                .build());
        DefaultRedisProvider provider = new DefaultRedisProvider(vertx, configurationProvider);
        RedisAPI oldAPI = get(provider.redis());
        RedisConnection oldConnection = get(provider.redisConnection());
        assertEquals("PONG", get(oldAPI.ping(List.of())).toString());
        NetSocket oldSocket = socket.get();

        respondToPing.set(false);
        pingReceived.set(false);
        Future<Response> outstanding = oldAPI.ping(List.of());
        await().atMost(Duration.ofSeconds(5)).untilTrue(pingReceived);

        int port = server.actualPort();
        get(server.close());
        get(oldSocket.close());
        try {
            get(outstanding);
            fail("A command on the closed connection must fail");
        } catch (ExecutionException expected) {
            assertTrue(outstanding.failed());
        }

        respondToPing.set(true);
        server = startServer(port);
        // No provider calls here: the scheduled reconnect must establish the new socket.
        await().atMost(Duration.ofSeconds(5)).until(() -> socket.get() != oldSocket);
        RedisAPI newAPI = get(provider.redis());
        RedisConnection newConnection = get(provider.redisConnection());
        assertNotSame(oldAPI, newAPI);
        assertNotSame(oldConnection, newConnection);
        assertEquals("PONG", get(newAPI.ping(List.of())).toString());
        assertEquals("PONG", get(newConnection.send(Request.cmd(Command.PING))).toString());
    }

    private NetServer startServer(int port) throws Exception {
        return get(vertx.createNetServer().connectHandler(connection -> {
            socket.set(connection);
            List<String> command = new ArrayList<>();
            int[] remaining = {0};
            RecordParser parser = RecordParser.newDelimited("\r\n", connection);
            parser.handler(record -> {
                String value = record.toString();
                if (remaining[0] == 0) {
                    remaining[0] = Integer.parseInt(value.substring(1));
                } else if (!value.startsWith("$")) {
                    command.add(value);
                    if (--remaining[0] == 0) {
                        respond(connection, command.get(0));
                        command.clear();
                    }
                }
            });
        }).listen(port, "127.0.0.1"));
    }

    private void respond(NetSocket connection, String command) {
        if ("HELLO".equalsIgnoreCase(command)) {
            connection.write("%1\r\n$5\r\nproto\r\n:3\r\n");
        } else if ("PING".equalsIgnoreCase(command)) {
            pingReceived.set(true);
            if (respondToPing.get()) {
                connection.write("+PONG\r\n");
            }
        } else {
            connection.write("+OK\r\n");
        }
    }

    private static <T> T get(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
}
