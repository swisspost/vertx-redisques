package org.swisspush.redisques.util;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link MessageUtil} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class MessageUtilTest {

    private Message<JsonObject> message;

    @Before
    public void setUp() {
        message = Mockito.mock(Message.class);
    }

    @Test
    public void testExtractFilterPattern(TestContext context) {
        when(message.body()).thenReturn(RedisquesAPI.buildCheckOperation()); // no payload object
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(message);
        context.assertTrue(result.isOk());
        context.assertFalse(result.getOk().isPresent());

        when(message.body()).thenReturn(RedisquesAPI.buildGetLockOperation("queue_x")); // payload object without filter property
        result = MessageUtil.extractFilterPattern(message);
        context.assertTrue(result.isOk());
        context.assertFalse(result.getOk().isPresent());

        when(message.body()).thenReturn(RedisquesAPI.buildGetAllLocksOperation("")); // payload object with empty filter property
        result = MessageUtil.extractFilterPattern(message);
        context.assertTrue(result.isOk());
        context.assertTrue(result.getOk().isPresent());

        when(message.body()).thenReturn(RedisquesAPI.buildGetAllLocksOperation("xyz(.*)")); // payload object with valid filter property
        result = MessageUtil.extractFilterPattern(message);
        context.assertTrue(result.isOk());
        context.assertTrue(result.getOk().isPresent());
        context.assertEquals("xyz(.*)", result.getOk().get().pattern());

        when(message.body()).thenReturn(RedisquesAPI.buildGetAllLocksOperation("xyz(.*")); // payload object with invalid filter property
        result = MessageUtil.extractFilterPattern(message);
        context.assertTrue(result.isErr());
        context.assertTrue(result.getErr().contains("Error while compile regex pattern"));
    }
}
