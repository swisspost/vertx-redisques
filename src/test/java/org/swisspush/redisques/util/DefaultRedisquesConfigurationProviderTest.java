package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for {@link DefaultRedisquesConfigurationProvider} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class DefaultRedisquesConfigurationProviderTest {

    private Vertx vertx;
    private DefaultRedisquesConfigurationProvider configurationProvider;

    @Before
    public void setup(TestContext context) {
        vertx = Vertx.vertx();
    }

    @Test
    public void testInitialConfiguration(TestContext context) {
        JsonObject config = new JsonObject("{\n" +
                "    \"address\": \"redisques\",\n" +
                "    \"configuration-updated-address\": \"redisques-configuration-updated\",\n" +
                "    \"redis-prefix\": \"redisques:\",\n" +
                "    \"processor-address\": \"redisques-processor\",\n" +
                "    \"refresh-period\": 10,\n" +
                "    \"redisHost\": \"localhost\",\n" +
                "    \"redisPort\": 6379,\n" +
                "    \"redisAuth\": null,\n" +
                "    \"checkInterval\": 60,\n" +
                "    \"processorTimeout\": 240000,\n" +
                "    \"processorDelayMax\": 23,\n" +
                "    \"httpRequestHandlerEnabled\": true,\n" +
                "    \"httpRequestHandlerPrefix\": \"/queuing\",\n" +
                "    \"httpRequestHandlerPort\": 7070,\n" +
                "    \"httpRequestHandlerUserHeader\": \"x-rp-usr\",\n" +
                "    \"queueConfigurations\": [\n" +
                "    ],\n" +
                "    \"enableQueueNameDecoding\": true,\n" +
                "    \"maxPoolSize\": 200,\n" +
                "    \"maxPoolWaitingSize\": -1,\n" +
                "    \"maxPipelineWaitingSize\": 2048,\n" +
                "    \"queueSpeedIntervalSec\": 60,\n" +
                "    \"memoryUsageLimitPercent\": 100,\n" +
                "    \"memoryUsageCheckIntervalSec\": 60\n" +
                "}\n");

        configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, config);
        RedisquesConfiguration conf = configurationProvider.configuration();
        context.assertEquals(60, conf.getCheckInterval());
        context.assertEquals(100, conf.getMemoryUsageLimitPercent());
        context.assertEquals("redisques", conf.getAddress());
        context.assertEquals("redisques:", conf.getRedisPrefix());
        context.assertTrue(conf.getHttpRequestHandlerEnabled());
        context.assertTrue(conf.getEnableQueueNameDecoding());
    }

    @Test
    public void testUpdateConfiguration(TestContext context) {
        JsonObject initialConfig = new JsonObject("{\n" +
                "    \"address\": \"redisques\",\n" +
                "    \"configuration-updated-address\": \"redisques-configuration-updated\",\n" +
                "    \"redis-prefix\": \"redisques:\",\n" +
                "    \"processor-address\": \"redisques-processor\",\n" +
                "    \"refresh-period\": 10,\n" +
                "    \"redisHost\": \"localhost\",\n" +
                "    \"redisPort\": 6379,\n" +
                "    \"redisAuth\": null,\n" +
                "    \"checkInterval\": 60,\n" +
                "    \"processorTimeout\": 240000,\n" +
                "    \"processorDelayMax\": 0,\n" +
                "    \"httpRequestHandlerEnabled\": true,\n" +
                "    \"httpRequestHandlerPrefix\": \"/queuing\",\n" +
                "    \"httpRequestHandlerPort\": 7070,\n" +
                "    \"httpRequestHandlerUserHeader\": \"x-rp-usr\",\n" +
                "    \"queueConfigurations\": [\n" +
                "    ],\n" +
                "    \"enableQueueNameDecoding\": true,\n" +
                "    \"maxPoolSize\": 200,\n" +
                "    \"maxPoolWaitingSize\": -1,\n" +
                "    \"maxPipelineWaitingSize\": 2048,\n" +
                "    \"queueSpeedIntervalSec\": 60,\n" +
                "    \"memoryUsageLimitPercent\": 100,\n" +
                "    \"memoryUsageCheckIntervalSec\": 60\n" +
                "}\n");

        configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, initialConfig);
        RedisquesConfiguration conf = configurationProvider.configuration();
        context.assertEquals(0L, conf.getProcessorDelayMax());

        JsonObject updateConfig = new JsonObject("{\n" +
                "  \"processorDelayMax\": 23\n" +
                "}");
        Result<Void, String> updateResult = configurationProvider.updateConfiguration(updateConfig, true);
        context.assertTrue(updateResult.isOk());

        conf = configurationProvider.configuration();
        context.assertEquals(60, conf.getCheckInterval());
        context.assertEquals(100, conf.getMemoryUsageLimitPercent());
        context.assertEquals("redisques", conf.getAddress());
        context.assertEquals("redisques:", conf.getRedisPrefix());
        context.assertTrue(conf.getHttpRequestHandlerEnabled());
        context.assertTrue(conf.getEnableQueueNameDecoding());

        Awaitility.await().atMost(Duration.ofSeconds(2)).until(
                () -> configurationProvider.configuration().getProcessorDelayMax() == 23L, equalTo(true));
    }

    @Test
    public void testInvalidUpdateDoesNotChangeConfiguration(TestContext context) {
        JsonObject initialConfig = new JsonObject("{\n" +
                "    \"address\": \"redisques\",\n" +
                "    \"configuration-updated-address\": \"redisques-configuration-updated\",\n" +
                "    \"redis-prefix\": \"redisques:\",\n" +
                "    \"processor-address\": \"redisques-processor\",\n" +
                "    \"refresh-period\": 10,\n" +
                "    \"redisHost\": \"localhost\",\n" +
                "    \"redisPort\": 6379,\n" +
                "    \"redisAuth\": null,\n" +
                "    \"checkInterval\": 60,\n" +
                "    \"processorTimeout\": 240000,\n" +
                "    \"processorDelayMax\": 55,\n" +
                "    \"httpRequestHandlerEnabled\": true,\n" +
                "    \"httpRequestHandlerPrefix\": \"/queuing\",\n" +
                "    \"httpRequestHandlerPort\": 7070,\n" +
                "    \"httpRequestHandlerUserHeader\": \"x-rp-usr\",\n" +
                "    \"queueConfigurations\": [\n" +
                "    ],\n" +
                "    \"enableQueueNameDecoding\": true,\n" +
                "    \"maxPoolSize\": 200,\n" +
                "    \"maxPoolWaitingSize\": -1,\n" +
                "    \"maxPipelineWaitingSize\": 2048,\n" +
                "    \"queueSpeedIntervalSec\": 60,\n" +
                "    \"memoryUsageLimitPercent\": 100,\n" +
                "    \"memoryUsageCheckIntervalSec\": 60\n" +
                "}\n");

        configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, initialConfig);
        RedisquesConfiguration conf = configurationProvider.configuration();
        context.assertEquals(55L, conf.getProcessorDelayMax());

        JsonObject updateConfig = new JsonObject("{\n" +
                "  \"foobar\": 23\n" +
                "}");
        Result<Void, String> updateResult = configurationProvider.updateConfiguration(updateConfig, true);
        context.assertTrue(updateResult.isErr());
        context.assertEquals("Not supported configuration values received: [foobar]", updateResult.getErr());

        conf = configurationProvider.configuration();
        context.assertEquals(60, conf.getCheckInterval());
        context.assertEquals(100, conf.getMemoryUsageLimitPercent());
        context.assertEquals("redisques", conf.getAddress());
        context.assertEquals("redisques:", conf.getRedisPrefix());
        context.assertTrue(conf.getHttpRequestHandlerEnabled());
        context.assertTrue(conf.getEnableQueueNameDecoding());

        Awaitility.await().atMost(Duration.ofSeconds(2)).until(
                () -> configurationProvider.configuration().getProcessorDelayMax() == 55L, equalTo(true));

        updateConfig = new JsonObject("{\n" +
                "  \"processorDelayMax\": \"not_a_number\"\n" +
                "}");
        updateResult = configurationProvider.updateConfiguration(updateConfig, true);
        context.assertTrue(updateResult.isErr());
        context.assertEquals("Value for configuration property 'processorDelayMax' is not a number", updateResult.getErr());

        conf = configurationProvider.configuration();
        context.assertEquals(60, conf.getCheckInterval());
        context.assertEquals(100, conf.getMemoryUsageLimitPercent());
        context.assertEquals("redisques", conf.getAddress());
        context.assertEquals("redisques:", conf.getRedisPrefix());
        context.assertTrue(conf.getHttpRequestHandlerEnabled());
        context.assertTrue(conf.getEnableQueueNameDecoding());

        Awaitility.await().atMost(Duration.ofSeconds(2)).until(
                () -> configurationProvider.configuration().getProcessorDelayMax() == 55L, equalTo(true));
    }

    @Test
    public void testInvalidUpdateConfigurationValueSetsDefault(TestContext context) {
        JsonObject initialConfig = new JsonObject("{\n" +
                "    \"address\": \"redisques\",\n" +
                "    \"configuration-updated-address\": \"redisques-configuration-updated\",\n" +
                "    \"redis-prefix\": \"redisques:\",\n" +
                "    \"processor-address\": \"redisques-processor\",\n" +
                "    \"refresh-period\": 10,\n" +
                "    \"redisHost\": \"localhost\",\n" +
                "    \"redisPort\": 6379,\n" +
                "    \"redisAuth\": null,\n" +
                "    \"checkInterval\": 60,\n" +
                "    \"processorTimeout\": 240000,\n" +
                "    \"processorDelayMax\": 55,\n" +
                "    \"httpRequestHandlerEnabled\": true,\n" +
                "    \"httpRequestHandlerPrefix\": \"/queuing\",\n" +
                "    \"httpRequestHandlerPort\": 7070,\n" +
                "    \"httpRequestHandlerUserHeader\": \"x-rp-usr\",\n" +
                "    \"queueConfigurations\": [\n" +
                "    ],\n" +
                "    \"enableQueueNameDecoding\": true,\n" +
                "    \"maxPoolSize\": 200,\n" +
                "    \"maxPoolWaitingSize\": -1,\n" +
                "    \"maxPipelineWaitingSize\": 2048,\n" +
                "    \"queueSpeedIntervalSec\": 60,\n" +
                "    \"memoryUsageLimitPercent\": 100,\n" +
                "    \"memoryUsageCheckIntervalSec\": 60\n" +
                "}\n");

        configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, initialConfig);
        RedisquesConfiguration conf = configurationProvider.configuration();
        context.assertEquals(55L, conf.getProcessorDelayMax());

        JsonObject updateConfig = new JsonObject("{\n" +
                "  \"processorDelayMax\": -23\n" +
                "}");
        Result<Void, String> updateResult = configurationProvider.updateConfiguration(updateConfig, true);
        context.assertTrue(updateResult.isOk());

        // the negative value should be changed to the default value of 0
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(
                () -> configurationProvider.configuration().getProcessorDelayMax() == 0L, equalTo(true));
    }

    @Test
    public void testEmptyConfiguration(TestContext context) {
        JsonObject config = new JsonObject("{}");

        configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, config);
        RedisquesConfiguration conf = configurationProvider.configuration();
        context.assertEquals(60, conf.getCheckInterval());
        context.assertEquals(100, conf.getMemoryUsageLimitPercent());
        context.assertEquals("redisques", conf.getAddress());
        context.assertEquals("redisques:", conf.getRedisPrefix());
        context.assertFalse(conf.getHttpRequestHandlerEnabled());
        context.assertTrue(conf.getEnableQueueNameDecoding());
    }

    @Test
    public void testUpdateConfigurationWithNull(TestContext context) {
        JsonObject initialConfig = new JsonObject("{\n" +
                "    \"address\": \"redisques\",\n" +
                "    \"configuration-updated-address\": \"redisques-configuration-updated\",\n" +
                "    \"redis-prefix\": \"redisques:\",\n" +
                "    \"processor-address\": \"redisques-processor\",\n" +
                "    \"refresh-period\": 10,\n" +
                "    \"redisHost\": \"localhost\",\n" +
                "    \"redisPort\": 6379,\n" +
                "    \"redisAuth\": null,\n" +
                "    \"checkInterval\": 60,\n" +
                "    \"processorTimeout\": 240000,\n" +
                "    \"processorDelayMax\": 0,\n" +
                "    \"httpRequestHandlerEnabled\": true,\n" +
                "    \"httpRequestHandlerPrefix\": \"/queuing\",\n" +
                "    \"httpRequestHandlerPort\": 7070,\n" +
                "    \"httpRequestHandlerUserHeader\": \"x-rp-usr\",\n" +
                "    \"queueConfigurations\": [\n" +
                "    ],\n" +
                "    \"enableQueueNameDecoding\": true,\n" +
                "    \"maxPoolSize\": 200,\n" +
                "    \"maxPoolWaitingSize\": -1,\n" +
                "    \"maxPipelineWaitingSize\": 2048,\n" +
                "    \"queueSpeedIntervalSec\": 60,\n" +
                "    \"memoryUsageLimitPercent\": 100,\n" +
                "    \"memoryUsageCheckIntervalSec\": 60\n" +
                "}\n");

        configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, initialConfig);
        RedisquesConfiguration conf = configurationProvider.configuration();
        context.assertEquals(0L, conf.getProcessorDelayMax());

        Result<Void, String> updateResult = configurationProvider.updateConfiguration(null, true);
        context.assertTrue(updateResult.isErr());
        context.assertEquals("Configuration values missing", updateResult.getErr());

        conf = configurationProvider.configuration();
        context.assertEquals(60, conf.getCheckInterval());
        context.assertEquals(100, conf.getMemoryUsageLimitPercent());
        context.assertEquals("redisques", conf.getAddress());
        context.assertEquals("redisques:", conf.getRedisPrefix());
        context.assertTrue(conf.getHttpRequestHandlerEnabled());
        context.assertTrue(conf.getEnableQueueNameDecoding());

        Awaitility.await().atMost(Duration.ofSeconds(2)).until(
                () -> configurationProvider.configuration().getProcessorDelayMax() == 0L, equalTo(true));
    }
}
