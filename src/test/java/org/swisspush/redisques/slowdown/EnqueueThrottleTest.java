package org.swisspush.redisques.slowdown;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.RedisquesAPI;
import org.swisspush.redisques.util.RedisquesConfiguration;

import java.util.Collections;
import java.util.HashSet;

import static org.swisspush.redisques.util.RedisquesAPI.*;

@RunWith(VertxUnitRunner.class)
public class EnqueueThrottleTest {

    private static final String REDISQUES_ADDRESS = "EnqueueThrottleTest-RedisQues";
    private static final String PROCESSOR_ADDRESS = "EnqueueThrottleTest-Processor";
    private static final String QUEUE_NAME = "EnqueueThrottleTest-q1";

    @ClassRule
    public static RunTestOnContext RULE = new RunTestOnContext(new VertxOptions().setBlockedThreadCheckInterval(10_000));

    @Before
    public void deployRedisQuesAndRegisterProcessor(TestContext testContext) {
        Async async = testContext.async();
        Vertx vertx = RULE.vertx();

        RedisquesConfiguration redisquesConfig = RedisquesConfiguration.with()
                .address(REDISQUES_ADDRESS)
                .processorAddress(PROCESSOR_ADDRESS)
                .httpRequestHandlerEnabled(true)
                .refreshPeriod(2)
                .checkInterval(10)
                .queueConfigurations(Collections.singletonList(
                        new QueueConfiguration().withPattern(QUEUE_NAME).withEnqueueDelayMillisPerSize(500).withEnqueueMaxDelayMillis(1700)
                ))
                .build();

        vertx.eventBus().consumer(PROCESSOR_ADDRESS, this::processor);

        vertx.deployVerticle(new RedisQues(), new DeploymentOptions().setConfig(redisquesConfig.asJsonObject()), done -> {
            JsonObject delete = RedisquesAPI.buildDeleteAllQueueItemsOperation(QUEUE_NAME);
            vertx.eventBus().send(REDISQUES_ADDRESS, delete, deleteDone -> async.complete());
        });
    }

    private static boolean blockProcessor = true;
    private static HashSet<String> queueMirror = new HashSet<>();

    private void processor(Message<JsonObject> msg) {
        String message = msg.body().getString("payload");
        if (!blockProcessor) {
            queueMirror.remove(message);
        }
        String replyStatus = blockProcessor ? ERROR : OK;
        System.out.println("Processor received: " + message + " and will reply: " + replyStatus);
        msg.reply(new JsonObject().put(STATUS, replyStatus));
    }

    @Test
    public void testEnqueueThrottling(TestContext testContext) {
        Async async = testContext.async();
        sendAMessage(testContext);
        Vertx vertx = RULE.vertx();
        vertx.setPeriodic(100, id -> {
            if (queueMirror.isEmpty()) {
                System.out.println("All messages (as in our queueMirror) processed --> done");
                async.complete();
            }
        });
    }

    private void sendAMessage(TestContext testContext) {
        int num = queueMirror.size();
        if (num == 6) { // the 5th and 6th message (Hallo-4 and Hallo-5) reach "enqueueMaxDelayMillis" - don't need more
            System.out.println("All messages enqueued - will now allow Processor to consume successfully");
            blockProcessor = false;
            return;
        }
        String message = "Hallo-" + num;
        queueMirror.add(message);
        System.out.println("enqueue " + message);
        JsonObject enque = RedisquesAPI.buildEnqueueOperation(QUEUE_NAME, message);
        long t0 = System.nanoTime();
        RULE.vertx().eventBus().send(REDISQUES_ADDRESS, enque, result -> {
            long enqueuDurationMillis = (System.nanoTime() - t0) / 1_000_000;
            long expectedDurationMillis = Math.min(num * 500, 1700); // see QueueConfiguration above
            if (Math.abs(enqueuDurationMillis - expectedDurationMillis) > 100) {
                testContext.fail("expected " + expectedDurationMillis + " but took " + enqueuDurationMillis);
            }
            System.out.println("enqueue " + message + " done after " + enqueuDurationMillis + " ms (expected " + expectedDurationMillis + " ms)");
            sendAMessage(testContext);
        });
    }
}
