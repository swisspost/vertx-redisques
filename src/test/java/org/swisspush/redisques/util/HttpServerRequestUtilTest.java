package org.swisspush.redisques.util;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;
import static org.swisspush.redisques.util.HttpServerRequestUtil.evaluateUrlParameterToBeEmptyOrTrue;
import static org.swisspush.redisques.util.HttpServerRequestUtil.extractNonEmptyJsonArrayFromBody;
import static org.swisspush.redisques.util.RedisquesAPI.LOCKS;

/**
 * Tests for {@link HttpServerRequestUtil} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class HttpServerRequestUtilTest {

    private HttpServerRequest request;

    @Before
    public void setUp() {
        request = Mockito.mock(HttpServerRequest.class);
    }

    @Test
    public void testEvaluateUrlParameterToBeEmptyOrTrue(TestContext context) {
        String paramX = "param_x";

        when(request.params()).thenReturn(new HeadersMultiMap());
        context.assertFalse(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new HeadersMultiMap().add(paramX, "false"));
        context.assertFalse(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new HeadersMultiMap().add(paramX, "someValue"));
        context.assertFalse(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new HeadersMultiMap().add(paramX, ""));
        context.assertTrue(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new HeadersMultiMap().add(paramX, "true"));
        context.assertTrue(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new HeadersMultiMap().add(paramX, "TRUE"));
        context.assertTrue(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new HeadersMultiMap().add(paramX, "trUe"));
        context.assertTrue(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));
    }

    @Test
    public void testExtractJsonArrayFromBody(TestContext context){
        Result<JsonArray, String> result = extractNonEmptyJsonArrayFromBody(LOCKS, "{\"locks\": [\"lock_1\", \"lock_2\", \"lock_3\"]}");
        context.assertTrue(result.isOk());
        context.assertEquals(new JsonArray().add("lock_1").add("lock_2").add("lock_3"), result.getOk());

        result = extractNonEmptyJsonArrayFromBody(LOCKS, "{\"locks\": []}");
        context.assertTrue(result.isErr());
        context.assertEquals("array 'locks' is not allowed to be empty", result.getErr());

        //invalid json
        result = extractNonEmptyJsonArrayFromBody(LOCKS, "{\"locks\": []");
        context.assertTrue(result.isErr());
        context.assertEquals("failed to parse request payload", result.getErr());

        //not an array
        result = extractNonEmptyJsonArrayFromBody(LOCKS, "{\"locks\": {}");
        context.assertTrue(result.isErr());
        context.assertEquals("failed to parse request payload", result.getErr());

        //no array called locks
        result = extractNonEmptyJsonArrayFromBody(LOCKS, "{\"abc\": []}");
        context.assertTrue(result.isErr());
        context.assertEquals("no array called 'locks' found", result.getErr());
    }

    @Test
    public void testBase64Decoder(TestContext context){
        String testDataUrlEncoded =  "{\n" +
                "  \"headers\": [\n" +
                "    [\n" +
                "      \"Content-Type\",\n" +
                "      \"application/json\"\n" +
                "    ],\n" +
                "    [\n" +
                "      \"Accept-Charset\",\n" +
                "      \"utf-8, iso-8859-1, utf-16, *;q=0.7\"\n" +
                "    ]\n" +
                "  ],\n" +
                "  \"queueTimestamp\": 1477983671291,\n" +
                "   \"payload\": \"5rWL6K-V\"\n" +
                "}";
        String testDataEncoded =  "{\n" +
                "  \"headers\": [\n" +
                "    [\n" +
                "      \"Content-Type\",\n" +
                "      \"application/json\"\n" +
                "    ],\n" +
                "    [\n" +
                "      \"Accept-Charset\",\n" +
                "      \"utf-8, iso-8859-1, utf-16, *;q=0.7\"\n" +
                "    ]\n" +
                "  ],\n" +
                "  \"queueTimestamp\": 1477983671291,\n" +
                "   \"payload\": \"5rWL6K+V\"\n" +
                "}";
        String decodedUrlPayload = HttpServerRequestUtil.decode(testDataUrlEncoded);
        String decodedPayload = HttpServerRequestUtil.decode(testDataEncoded);
        context.assertEquals(decodedUrlPayload, decodedPayload);
    }
}
