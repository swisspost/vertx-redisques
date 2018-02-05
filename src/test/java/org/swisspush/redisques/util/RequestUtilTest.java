package org.swisspush.redisques.util;

import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;
import static org.swisspush.redisques.util.RequestUtil.evaluateUrlParameterToBeEmptyOrTrue;

/**
 * Tests for {@link RequestUtil} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class RequestUtilTest {

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
}
