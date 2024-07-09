package org.swisspush.redisques.util;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for {@link StatusCode} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class StatuscodeTest {

    @Test
    public void testGetStatusCodeAndStatusMessage(TestContext context){
        context.assertEquals(200, StatusCode.OK.getStatusCode());
        context.assertEquals("OK", StatusCode.OK.getStatusMessage());

        context.assertEquals(202, StatusCode.ACCEPTED.getStatusCode());
        context.assertEquals("Accepted", StatusCode.ACCEPTED.getStatusMessage());

        context.assertEquals(304, StatusCode.NOT_MODIFIED.getStatusCode());
        context.assertEquals("Not Modified", StatusCode.NOT_MODIFIED.getStatusMessage());

        context.assertEquals(400, StatusCode.BAD_REQUEST.getStatusCode());
        context.assertEquals("Bad Request", StatusCode.BAD_REQUEST.getStatusMessage());

        context.assertEquals(403, StatusCode.FORBIDDEN.getStatusCode());
        context.assertEquals("Forbidden", StatusCode.FORBIDDEN.getStatusMessage());

        context.assertEquals(404, StatusCode.NOT_FOUND.getStatusCode());
        context.assertEquals("Not Found", StatusCode.NOT_FOUND.getStatusMessage());

        context.assertEquals(405, StatusCode.METHOD_NOT_ALLOWED.getStatusCode());
        context.assertEquals("Method Not Allowed", StatusCode.METHOD_NOT_ALLOWED.getStatusMessage());

        context.assertEquals(409, StatusCode.CONFLICT.getStatusCode());
        context.assertEquals("Conflict", StatusCode.CONFLICT.getStatusMessage());

        context.assertEquals(500, StatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        context.assertEquals("Internal Server Error", StatusCode.INTERNAL_SERVER_ERROR.getStatusMessage());

        context.assertEquals(503, StatusCode.SERVICE_UNAVAILABLE.getStatusCode());
        context.assertEquals("Service Unavailable", StatusCode.SERVICE_UNAVAILABLE.getStatusMessage());

        context.assertEquals(504, StatusCode.TIMEOUT.getStatusCode());
        context.assertEquals("Gateway Timeout", StatusCode.TIMEOUT.getStatusMessage());

        context.assertEquals(507, StatusCode.INSUFFICIENT_STORAGE.getStatusCode());
        context.assertEquals("Insufficient Storage", StatusCode.INSUFFICIENT_STORAGE.getStatusMessage());
    }

    @Test
    public void testStatusCodeFromCode(TestContext context){
        context.assertNull(StatusCode.fromCode(0));
        context.assertNull(StatusCode.fromCode(999));
        context.assertEquals(StatusCode.OK, StatusCode.fromCode(200));
        context.assertEquals(StatusCode.BAD_REQUEST, StatusCode.fromCode(400));
        context.assertEquals(StatusCode.METHOD_NOT_ALLOWED, StatusCode.fromCode(405));
        context.assertEquals(StatusCode.SERVICE_UNAVAILABLE, StatusCode.fromCode(503));
    }

    @Test
    public void testStatusCodeToString(TestContext context){
        context.assertEquals("200 OK", StatusCode.OK.toString());
        context.assertEquals("304 Not Modified", StatusCode.NOT_MODIFIED.toString());
        context.assertEquals("500 Internal Server Error", StatusCode.INTERNAL_SERVER_ERROR.toString());
        context.assertEquals("504 Gateway Timeout", StatusCode.TIMEOUT.toString());
    }
}
