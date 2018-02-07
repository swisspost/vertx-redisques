package org.swisspush.redisques.util;

import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpServerRequest;
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
 * @author https://github.com/mcweba [Marc-Andre Weber]
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

        when(request.params()).thenReturn(new CaseInsensitiveHeaders());
        context.assertFalse(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new CaseInsensitiveHeaders().add(paramX, "false"));
        context.assertFalse(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new CaseInsensitiveHeaders().add(paramX, "someValue"));
        context.assertFalse(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new CaseInsensitiveHeaders().add(paramX, ""));
        context.assertTrue(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new CaseInsensitiveHeaders().add(paramX, "true"));
        context.assertTrue(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new CaseInsensitiveHeaders().add(paramX, "TRUE"));
        context.assertTrue(evaluateUrlParameterToBeEmptyOrTrue(paramX, request));

        when(request.params()).thenReturn(new CaseInsensitiveHeaders().add(paramX, "trUe"));
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
}
