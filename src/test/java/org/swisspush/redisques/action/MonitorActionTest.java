package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.RedisquesConfiguration;

import static org.mockito.Mockito.*;
import static org.swisspush.redisques.util.RedisquesAPI.buildMonitorOperation;

/**
 * Tests for {@link MonitorAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class MonitorActionTest {

    private HttpClient httpClient;
    private RedisquesConfiguration configuration;
    private MonitorAction action;
    private Message<JsonObject> message;

    private HttpClientRequest clientRequest;
    private HttpClientResponse clientResponse;

    @Before
    public void setup() {
        configuration = new RedisquesConfiguration();
        httpClient = Mockito.mock(HttpClient.class);
        action = new MonitorAction(configuration, httpClient, Mockito.mock(Logger.class));
        message = Mockito.mock(Message.class);
    }

    @Test
    public void testDisabledHttpRequestHandler(){
        when(message.body()).thenReturn(buildMonitorOperation(true, 5));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(
                Buffer.buffer("{\"status\":\"error\",\"message\":\"HttpRequestHandler is disabled\"}"))));
        verifyNoInteractions(httpClient);
    }

    @Test
    public void testFailingRequestToMonitorEndpoint(){
        configuration = RedisquesConfiguration.with()
                .httpRequestHandlerEnabled(true)
                .build();
        action = new MonitorAction(configuration, httpClient, Mockito.mock(Logger.class));

        when(httpClient.request(eq(HttpMethod.GET), anyInt(), anyString(), anyString()))
                .thenReturn(Future.failedFuture("booom"));

        when(message.body()).thenReturn(buildMonitorOperation(true, 5));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(
                Buffer.buffer("{\"status\":\"error\",\"message\":\"booom\"}"))));
    }

    @Test
    public void testSuccessMonitorRequest(){
        configuration = RedisquesConfiguration.with()
                .httpRequestHandlerEnabled(true)
                .build();
        action = new MonitorAction(configuration, httpClient, Mockito.mock(Logger.class));

        clientRequest = Mockito.mock(HttpClientRequest.class);
        clientResponse = Mockito.mock(HttpClientResponse.class);
        Mockito.when(clientRequest.headers()).thenReturn(new HeadersMultiMap());
        Mockito.when(clientRequest.response()).thenReturn(Future.succeededFuture(clientResponse));

        String expectedNoEmptyQueuesNoLimit = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when(clientResponse.statusCode()).thenReturn(200);
        when(clientResponse.body()).thenReturn(Future.succeededFuture(Buffer.buffer(expectedNoEmptyQueuesNoLimit)));

        when(httpClient.request(eq(HttpMethod.GET), anyInt(), anyString(), anyString()))
                .thenReturn(Future.succeededFuture(clientRequest));
        when(clientRequest.send()).thenReturn(Future.succeededFuture(clientResponse));

        when(message.body()).thenReturn(buildMonitorOperation(true, 5));

        action.execute(message);

        verify(message, times(1)).reply(eq(new JsonObject(
                Buffer.buffer("{\"status\":\"ok\",\"value\":{\"queues\":[{\"name\":\"queue_1\",\"size\":3}," +
                        "{\"name\":\"queue_2\",\"size\":2}]}}"))));
    }
}
