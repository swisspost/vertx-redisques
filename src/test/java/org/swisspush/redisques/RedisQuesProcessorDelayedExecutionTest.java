package org.swisspush.redisques;

import org.awaitility.Awaitility;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.util.RedisquesConfiguration;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class RedisQuesProcessorDelayedExecutionTest extends AbstractTestCase {

    private static MessageConsumer<JsonObject> queueProcessor = null;

    private static final String CUSTOM_REDIS_KEY_PREFIX = "mycustomredisprefix:";
    private static final String CUSTOM_REDISQUES_ADDRESS = "customredisques";

    @Override
    protected String getRedisPrefix() {
        return CUSTOM_REDIS_KEY_PREFIX;
    }

    @Override
    protected String getRedisquesAddress() {
        return CUSTOM_REDISQUES_ADDRESS;
    }

    @Rule
    public Timeout rule = Timeout.seconds(300);

    @Before
    public void createQueueProcessor(TestContext context) {
        deployRedisques(context);
        queueProcessor = vertx.eventBus().consumer("processor-address");
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    protected void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();
        JsonObject config = RedisquesConfiguration.with()
                .address(getRedisquesAddress())
                .redisPrefix(CUSTOM_REDIS_KEY_PREFIX)
                .processorAddress("processor-address")
                .refreshPeriod(2)
                .processorDelayMax(1500)
                .build()
                .asJsonObject();

        RedisQues redisQues = new RedisQues();
        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - {} was successful.", redisQues.getClass().getSimpleName());
            jedis = new Jedis("localhost", 6379, 5000);
        }));
    }

    @Test
    public void queueProcessorShouldBeNotifiedWithNonLockedQueue(TestContext context) {
        Async async = context.async();
        flushAll();

        String queue = "queue1";
        final AtomicBoolean processorCalled = new AtomicBoolean(false);

        long start = System.currentTimeMillis();

        queueProcessor.handler(event -> {
            long duration = System.currentTimeMillis() - start;
            context.assertTrue(duration < 1600, "QueueProcessor should have been called at the latest after 1600ms");
            processorCalled.set(true);
        });

        eventBusSend(buildEnqueueOperation(queue, "hello"), reply ->
                context.assertEquals(OK, reply.result().body().getString(STATUS)));

        // after at most 5 seconds, the processor-address consumer should have been called
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(processorCalled::get, equalTo(true));

        async.complete();
    }
}
