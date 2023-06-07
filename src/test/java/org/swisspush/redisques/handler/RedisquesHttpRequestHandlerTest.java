package org.swisspush.redisques.handler;

import com.sun.istack.NotNull;
import com.sun.istack.Nullable;
import io.restassured.RestAssured;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.*;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.util.*;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static java.lang.System.currentTimeMillis;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsEmptyCollection.emptyCollectionOf;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Tests for the {@link RedisquesHttpRequestHandler} class
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesHttpRequestHandlerTest extends AbstractTestCase {
    private static String deploymentId = "";
    private Vertx testVertx;
    private TestMemoryUsageProvider memoryUsageProvider;

    private final String queueItemValid = "{\n" +
            "  \"method\": \"PUT\",\n" +
            "  \"uri\": \"/some/url/123/456\",\n" +
            "  \"headers\": [\n" +
            "    [\n" +
            "      \"Accept\",\n" +
            "      \"text/plain, */*; q=0.01\"\n" +
            "    ],\n" +
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
            "  \"payloadObject\": {\n" +
            "    \"actionTime\": \"2016-11-01T08:00:02.024+01:00\",\n" +
            "    \"type\": 2\n" +
            "  }\n" +
            "}";

    private final String queueItemValid2 = "{\n" +
            "  \"method\": \"PUT\",\n" +
            "  \"uri\": \"/some/url/333/555\",\n" +
            "  \"headers\": [\n" +
            "    [\n" +
            "      \"Accept\",\n" +
            "      \"text/plain, */*; q=0.01\"\n" +
            "    ],\n" +
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
            "  \"payloadObject\": {\n" +
            "    \"actionTime\": \"2016-11-01T08:00:02.024+01:00\",\n" +
            "    \"type\": 2\n" +
            "  }\n" +
            "}";

    private final String queueItemInvalid = "\n" +
            "  \"method\": \"PUT\",\n" +
            "  \"uri\": \"/some/url/123/456\",\n" +
            "  \"headers\": [\n" +
            "    [\n" +
            "      \"Accept\",\n" +
            "      \"text/plain, */*; q=0.01\"\n" +
            "    ],\n" +
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
            "  \"payloadObject\": {\n" +
            "    \"actionTime\": \"2016-11-01T08:00:02.024+01:00\",\n" +
            "    \"type\": 2\n" +
            "  }\n" +
            "}";

    private static final String configurationValid = "{\"processorDelayMax\":99}";
    private static final String configurationValidZero = "{\"processorDelayMax\":0}";
    private static final String configurationNotSupportedValues = "{\"processorDelayMax\":0, \"redisHost\":\"localhost\"}";
    private static final String configurationEmpty = "{}";

    @Rule
    public Timeout rule = Timeout.seconds(15);

    @BeforeClass
    public static void beforeClass() {
        RestAssured.baseURI = "http://127.0.0.1/";
        RestAssured.port = 7070;
    }

    @Before
    public void deployRedisques(TestContext context) {
        Async async = context.async();
        testVertx = Vertx.vertx();
        JsonObject config = RedisquesConfiguration.with()
                .address(getRedisquesAddress())
                .processorAddress("processor-address")
                .maxPoolSize(200)
                .memoryUsageLimitPercent(80)
                .maxPoolWaitSize(-1)
                .maxPipelineWaitSize(2048)
                .refreshPeriod(2)
                .httpRequestHandlerEnabled(true)
                .httpRequestHandlerPort(7070)
                .queueConfigurations(List.of(
                        new QueueConfiguration().withPattern("queue_1").withRetryIntervals(1, 2, 3, 5),
                        new QueueConfiguration().withPattern("stat.*").withRetryIntervals(1, 2, 3, 5)
                                .withEnqueueDelayMillisPerSize(5).withEnqueueMaxDelayMillis(100)
                ))
                .queueSpeedIntervalSec(4)
                .build()
                .asJsonObject();

        memoryUsageProvider = new TestMemoryUsageProvider(Optional.of(50));
        RedisQues redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(testVertx, config))
                .build();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            delay(1000);
            async.complete();
        }));
        async.awaitSuccess();
    }

    @After
    public void tearDown(TestContext context) {
        testVertx.undeploy(deploymentId, context.asyncAssertSuccess(Void -> {
            testVertx.close(context.asyncAssertSuccess());
            context.async().complete();
        }));
    }

    protected void eventBusSend(JsonObject operation, Handler<AsyncResult<Message<JsonObject>>> handler) {
        if (handler == null) {
            testVertx.eventBus().request(getRedisquesAddress(), operation);
        } else {
            testVertx.eventBus().request(getRedisquesAddress(), operation, handler);
        }
    }

    /**
     * Helper method to register an eventbus queue message handler which consumes just all
     * queue messages properly.
     * <p>
     * Note: don't forget to unregister the given MessageConsumer after test completion. Else this
     * might influence other tests later.
     *
     * @return The MessageConsumer used for later unregistration of the consumer.
     */
    private MessageConsumer registerQueueEventOkConsumer() {
        return testVertx.eventBus().consumer("processor-address",
                handler -> handler.reply(new JsonObject().put(RedisquesAPI.STATUS, RedisquesAPI.OK))
        );
    }

    @Test
    public void testUnknownRequestUrl(TestContext context) {
        Async async = context.async();
        when()
                .get("/an/unknown/path/")
                .then().assertThat()
                .statusCode(405);
        async.complete();
    }

    @Test
    public void listEndpoints(TestContext context) {
        when().get("/queuing/")
                .then().assertThat()
                .statusCode(200)
                .body("queuing", hasItems("locks/", "queues/", "monitor/", "configuration/"));
    }

    @Test
    public void getConfiguration(TestContext context) {
        when()
                .get("/queuing/configuration/")
                .then().assertThat()
                .statusCode(200)
                .body("any { it.key == 'redisHost' }", is(true)) // checks whether the property 'redisHost' exists. Ignoring the value
                .body("any { it.key == 'redisPort' }", is(true))
                .body("any { it.key == 'redis-prefix' }", is(true))
                .body("any { it.key == 'address' }", is(true))
                .body("any { it.key == 'configuration-updated-address' }", is(true))
                .body("any { it.key == 'processor-address' }", is(true))
                .body("any { it.key == 'refresh-period' }", is(true))
                .body("any { it.key == 'checkInterval' }", is(true))
                .body("any { it.key == 'processorTimeout' }", is(true))
                .body("any { it.key == 'processorDelayMax' }", is(true))
                .body("any { it.key == 'queueConfigurations' }", is(true))
                .body("any { it.key == 'enableQueueNameDecoding' }", is(true))
                .body("any { it.key == 'maxPoolSize' }", is(true))
                .body("any { it.key == 'maxPoolWaitingSize' }", is(true))
                .body("any { it.key == 'maxPipelineWaitingSize' }", is(true))
                .body("any { it.key == 'queueSpeedIntervalSec' }", is(true))
                .body("any { it.key == 'memoryUsageLimitPercent' }", is(true))
                .body("any { it.key == 'memoryUsageCheckIntervalSec' }", is(true))
                .body("any { it.key == 'httpRequestHandlerEnabled' }", is(true))
                .body("any { it.key == 'httpRequestHandlerPrefix' }", is(true))
                .body("any { it.key == 'httpRequestHandlerPort' }", is(true))
                .body("any { it.key == 'httpRequestHandlerUserHeader' }", is(true));
    }

    @Test
    public void setConfiguration(TestContext context) {
        when()
                .get("/queuing/configuration/")
                .then().assertThat()
                .statusCode(200)
                .body("processorDelayMax", equalTo(0));

        // provide a valid configuration. this should change the value of the property
        given().body(configurationValid).when().post("/queuing/configuration/").then().assertThat().statusCode(200);
        when()
                .get("/queuing/configuration/")
                .then().assertThat()
                .statusCode(200)
                .body("processorDelayMax", equalTo(99));

        // provide not supported configuration values. this should not change the value of the property
        given().body(configurationNotSupportedValues).when().post("/queuing/configuration/")
                .then().assertThat().statusCode(400).body(containsString("Not supported configuration values received: [redisHost]"));
        when()
                .get("/queuing/configuration/")
                .then().assertThat()
                .statusCode(200)
                .body("processorDelayMax", equalTo(99));

        // provide empty configuration values (missing processorDelayMax property). this should not change the value of the property
        given().body(configurationEmpty).when().post("/queuing/configuration/")
                .then().assertThat().statusCode(400).body(containsString("Value for configuration property 'processorDelayMax' is missing"));
        when()
                .get("/queuing/configuration/")
                .then().assertThat()
                .statusCode(200)
                .body("processorDelayMax", equalTo(99));

        // again provide a valid configuration. this should change the value of the property
        given().body(configurationValidZero).when().post("/queuing/configuration/").then().assertThat().statusCode(200);
        when()
                .get("/queuing/configuration/")
                .then().assertThat()
                .statusCode(200)
                .body("processorDelayMax", equalTo(0));
    }

    @Test
    public void getQueuesCount(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue_1", "item1_1"), m1 -> {
            eventBusSend(buildEnqueueOperation("queue_2", "item2_1"), m2 -> {
                eventBusSend(buildEnqueueOperation("queue_3", "item3_1"), m3 -> {
                    when()
                            .get("/queuing/queues/?count")
                            .then().assertThat()
                            .statusCode(200)
                            .body("count", equalTo(3));
                    async.complete();
                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void getQueuesCountEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue_1", "item1_1"), m1 -> {
            eventBusSend(buildEnqueueOperation("queue_2", "item2_1"), m2 -> {
                eventBusSend(buildEnqueueOperation("queue{3}", "item3_1"), m3 -> {
                    when()
                            .get("/queuing/queues/?count")
                            .then().assertThat()
                            .statusCode(200)
                            .body("count", equalTo(3));
                    async.complete();
                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void getQueuesCountNoQueues(TestContext context) {
        Async async = context.async();
        flushAll();
        when()
                .get("/queuing/queues/?count")
                .then().assertThat()
                .statusCode(200)
                .body("count", equalTo(0));
        async.complete();
    }

    @Test
    public void getQueuesCountFiltered(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("aaa", "item1_1"), m1 -> {
            eventBusSend(buildEnqueueOperation("aab", "item2_1"), m2 -> {
                eventBusSend(buildEnqueueOperation("abc", "item3_1"), m3 -> {

                    given().param(FILTER, "x").param(COUNT, true).when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("count", equalTo(0));

                    given().param(FILTER, "a").param(COUNT, true).when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("count", equalTo(3));

                    given().param(FILTER, "ab").param(COUNT, true).when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("count", equalTo(2));

                    given().param(FILTER, "c").param(COUNT, true).when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("count", equalTo(1));

                    given().param(FILTER, "c(.*").param(COUNT, true).when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(400);

                    async.complete();
                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void listQueues(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue_1", "item1_1"), m1 -> {
            eventBusSend(buildEnqueueOperation("queue_2", "item2_1"), m2 -> {
                eventBusSend(buildEnqueueOperation("queue_{with_special_chars}", "item3_1"), m3 -> {
                    when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(
                                    "queues", hasItems("queue_1", "queue_2", "queue_{with_special_chars}")
                            );
                    async.complete();
                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void listQueuesFiltered(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("aaa", "item1_1"), m1 -> {
            eventBusSend(buildEnqueueOperation("aab", "item2_1"), m2 -> {
                eventBusSend(buildEnqueueOperation("abc", "item3_1"), m3 -> {

                    given().param(FILTER, "x").when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("queues", is(emptyCollectionOf(String.class)));

                    given().param(FILTER, "ab").when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(
                                    "queues", hasItems("aab", "abc"),
                                    "queues", not(hasItem("aaa"))
                            );

                    given().param(FILTER, "a").when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(
                                    "queues", hasItems("aaa", "aab", "abc")
                            );

                    given().param(FILTER, "a(.*").when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(400);

                    async.complete();
                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void enqueueValidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        assertLockDoesNotExist(context, queueName);

        async.complete();
    }

    @Test
    public void enqueueValidBodyWithMemoryLimitReached(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        memoryUsageProvider.setCurrentMemoryUsage(Optional.of(90));

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/").then().assertThat().statusCode(507);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        memoryUsageProvider.setCurrentMemoryUsage(Optional.of(30));

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        assertLockDoesNotExist(context, queueName);

        async.complete();
    }

    @Test
    public void enqueueValidBodyEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue%7BEnqueue%7D";
        String queueNameDecoded = "queue{Enqueue}";
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().urlEncodingEnabled(false).body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueNameDecoded));

        given().urlEncodingEnabled(false).body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueNameDecoded));

        assertLockDoesNotExist(context, queueNameDecoded);

        async.complete();
    }

    @Test
    public void enqueueInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemInvalid).when().put("/queuing/enqueue/" + queueName + "/").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        assertLockDoesNotExist(context, queueName);

        async.complete();
    }

    @Test
    public void lockedEnqueueValidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        assertLockDoesNotExist(context, queueName);

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/?locked").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));
        assertLockExists(context, queueName);
        assertLockContent(context, queueName, "Unknown");

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/?locked").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void lockedEnqueueValidBodyWithMemoryLimitReached(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        assertLockDoesNotExist(context, queueName);

        memoryUsageProvider.setCurrentMemoryUsage(Optional.of(90));

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/?locked").then().assertThat().statusCode(507);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        assertLockDoesNotExist(context, queueName);

        memoryUsageProvider.setCurrentMemoryUsage(Optional.of(60));

        given().body(queueItemValid).when().put("/queuing/enqueue/" + queueName + "/?locked").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));
        assertLockExists(context, queueName);
        assertLockContent(context, queueName, "Unknown");

        async.complete();
    }

    @Test
    public void lockedEnqueueValidBodyRequestedByHeader(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = currentTimeMillis();
        String queueName = "queue_" + ts;
        String requestedBy = "user_" + ts;
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        assertLockDoesNotExist(context, queueName);

        given()
                .header("x-rp-usr", requestedBy)
                .body(queueItemValid)
                .when()
                .put("/queuing/enqueue/" + queueName + "/?locked")
                .then().assertThat().statusCode(200);

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));
        assertLockExists(context, queueName);
        assertLockContent(context, queueName, requestedBy);

        async.complete();
    }

    @Test
    public void lockedEnqueueInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        assertLockDoesNotExist(context, queueName);

        given().body(queueItemInvalid).when().put("/queuing/enqueue/" + queueName + "/?locked").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        assertLockDoesNotExist(context, queueName);

        async.complete();
    }

    @Test
    public void addQueueItemValidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void addQueueItemValidBodyEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue%7BEnqueue%7D";
        String queueNameDecoded = "queue{Enqueue}";
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueNameDecoded, 0);

        given().urlEncodingEnabled(false).body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueNameDecoded));

        given().urlEncodingEnabled(false).body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueNameDecoded));

        async.complete();
    }

    @Test
    public void addQueueItemInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemInvalid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void getSingleQueueItemWithNonNumericIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // try to get with non-numeric index
        String nonnumericIndex = "xx";
        when().get("/queuing/queues/" + queueName + "/" + nonnumericIndex).then().assertThat().statusCode(405);

        async.complete();
    }

    @Test
    public void getSingleQueueItemWithNonExistingIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // try to get with not existing index
        String notExistingIndex = "10";
        when().get("/queuing/queues/" + queueName + "/" + notExistingIndex).then().assertThat().statusCode(404).body(containsString("Not Found"));

        async.complete();
    }

    @Test
    public void getSingleQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "0";
        when().get("/queuing/queues/" + queueName + "/" + numericIndex).then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void getSingleQueueItemEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue%7BEnqueue%7D";
        String queueNameDecoded = "queue{Enqueue}";
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueNameDecoded, 0);

        given().urlEncodingEnabled(false).body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueNameDecoded));

        String numericIndex = "0";
        given().urlEncodingEnabled(false).when().get("/queuing/queues/" + queueName + "/" + numericIndex).then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItemOfUnlockedQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid2).when().put("/queuing/queues/" + queueName + "/0").then().assertThat().statusCode(409).body(containsString("Queue must be locked to perform this operation"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/" + queueName + "/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        // replacing with an invalid resource
        given().body(queueItemInvalid).when().put("/queuing/queues/" + queueName + "/0").then().assertThat().statusCode(409).body(containsString("Queue must be locked to perform this operation"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItemWithInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemInvalid).when().put("/queuing/queues/" + queueName + "/0").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/" + queueName + "/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItemWithNotExistingIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid2).when().put("/queuing/queues/" + queueName + "/10").then().assertThat().statusCode(404).body(containsString("Not Found"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/" + queueName + "/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid2).when().put("/queuing/queues/" + queueName + "/0").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/" + queueName + "/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid2).toString()));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItemEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue%7BEnqueue%7D";
        String queueNameDecoded = "queue{Enqueue}";
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueNameDecoded, 0);

        // lock queue
        given().urlEncodingEnabled(false).body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().urlEncodingEnabled(false).body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueNameDecoded));

        given().urlEncodingEnabled(false).body(queueItemValid2).when().put("/queuing/queues/" + queueName + "/0").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueNameDecoded));

        // check queue item has not been replaced
        given().urlEncodingEnabled(false).when().get("/queuing/queues/" + queueName + "/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid2).toString()));

        async.complete();
    }


    @Test
    public void deleteQueueItemWithNonNumericIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // try to delete with non-numeric index
        String nonnumericIndex = "xx";
        when().delete("/queuing/queues/" + queueName + "/" + nonnumericIndex).then().assertThat().statusCode(405);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItemOfUnlockedQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "22";
        when().delete("/queuing/queues/" + queueName + "/" + numericIndex).then().assertThat().statusCode(409).body(containsString("Queue must be locked to perform this operation"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItemNonExistingIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "22";
        when().delete("/queuing/queues/" + queueName + "/" + numericIndex).then().assertThat().statusCode(404).body(containsString("Not Found"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/" + queueName + "/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "0";
        when().delete("/queuing/queues/" + queueName + "/" + numericIndex).then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // try to delete again
        when().delete("/queuing/queues/" + queueName + "/" + numericIndex).then().assertThat().statusCode(404).body(containsString("Not Found"));

        async.complete();
    }

    @Test
    public void getQueueItemsCount(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue2"), message2 -> {
                when()
                        .get("/queuing/queues/queueEnqueue?count")
                        .then().assertThat()
                        .statusCode(200)
                        .body("count", equalTo(2));
                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void getQueueItemsCountEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue{Enqueue}", "helloEnqueue"), message -> {
            eventBusSend(buildEnqueueOperation("queue{Enqueue}", "helloEnqueue2"), message2 -> {
                given().urlEncodingEnabled(false).when()
                        .get("/queuing/queues/queue%7BEnqueue%7D?count")
                        .then().assertThat()
                        .statusCode(200)
                        .body("count", equalTo(2));
                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void getQueueItemsCountOfUnknownQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        when()
                .get("/queuing/queues/unknownQueue?count")
                .then().assertThat()
                .statusCode(200)
                .body("count", equalTo(0));
        async.complete();
    }

    @Test
    public void listQueueItems(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue2"), message2 -> {
                when().get("/queuing/queues/queueEnqueue")
                        .then().assertThat()
                        .statusCode(200)
                        .body("queueEnqueue", hasItems("helloEnqueue", "helloEnqueue2"));
                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void listQueueItemsEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue{Enqueue}", "helloEnqueue"), message -> {
            eventBusSend(buildEnqueueOperation("queue{Enqueue}", "helloEnqueue2"), message2 -> {
                given().urlEncodingEnabled(false).when().get("/queuing/queues/queue%7BEnqueue%7D")
                        .then().assertThat()
                        .statusCode(200)
                        .body("'queue{Enqueue}'", hasItems("helloEnqueue", "helloEnqueue2"));
                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void listQueueItemsWithLimitParameter(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue1"), m1 -> {
            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue2"), m2 -> {
                eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue3"), m3 -> {
                    eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue4"), m4 -> {
                        given()
                                .param("limit", 3)
                                .when()
                                .get("/queuing/queues/queueEnqueue")
                                .then().assertThat()
                                .statusCode(200)
                                .body(
                                        "queueEnqueue", hasItems("helloEnqueue1", "helloEnqueue2", "helloEnqueue3"),
                                        "queueEnqueue", not(hasItem("helloEnqueue4"))
                                );
                        async.complete();
                    });
                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void listQueueItemsOfUnknownQueue(TestContext context) {
        Async async = context.async();
        when().get("/queuing/queues/unknownQueue")
                .then().assertThat()
                .statusCode(200)
                .body("unknownQueue", empty());
        async.complete();
    }

    @Test
    public void deleteAllQueueItems(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);

            // delete all queue items
            when().delete("/queuing/queues/queueEnqueue")
                    .then().assertThat()
                    .statusCode(200);
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

            // delete all queue items again
            when().delete("/queuing/queues/queueEnqueue")
                    .then().assertThat()
                    .statusCode(404);
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

            async.complete();
        });
        async.awaitSuccess();
    }

    @Test
    public void deleteAllQueueItemsWithUnlockOfNonExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);

            // delete all queue items
            when().delete("/queuing/queues/queueEnqueue?unlock")
                    .then().assertThat()
                    .statusCode(200);
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

            // delete all queue items again
            when().delete("/queuing/queues/queueEnqueue")
                    .then().assertThat()
                    .statusCode(404);
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

            async.complete();
        });
        async.awaitSuccess();
    }

    @Test
    public void deleteAllQueueItemsWithNoUnlockOfExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();

        eventBusSend(buildPutLockOperation("queueEnqueue", "someuser"), putLockMessage -> {
            assertLockExists(context, "queueEnqueue");

            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);

                // delete all queue items
                when().delete("/queuing/queues/queueEnqueue")
                        .then().assertThat()
                        .statusCode(200);
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                assertLockExists(context, "queueEnqueue");

                // delete all queue items again
                when().delete("/queuing/queues/queueEnqueue")
                        .then().assertThat()
                        .statusCode(404);
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                assertLockExists(context, "queueEnqueue");

                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void deleteAllQueueItemsWithDoUnlockOfExistingLock(TestContext context) {
        Async async = context.async();
        flushAll();

        eventBusSend(buildPutLockOperation("queueEnqueue", "someuser"), putLockMessage -> {
            assertLockExists(context, "queueEnqueue");

            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);

                // delete all queue items
                when().delete("/queuing/queues/queueEnqueue?unlock")
                        .then().assertThat()
                        .statusCode(200);
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                assertLockDoesNotExist(context, "queueEnqueue");

                // delete all queue items again
                when().delete("/queuing/queues/queueEnqueue")
                        .then().assertThat()
                        .statusCode(404);
                assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
                assertLockDoesNotExist(context, "queueEnqueue");

                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void deleteAllQueueItemsOfNonExistingQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        when().delete("/queuing/queues/notExistingQueue_" + System.currentTimeMillis())
                .then().assertThat()
                .statusCode(404);
        async.complete();
    }

    @Test
    public void bulkDeleteQueues(TestContext context) {
        Async async = context.async();
        flushAll();

        eventBusSend(buildEnqueueOperation("q1", "q1_message"), e1 -> {
            eventBusSend(buildEnqueueOperation("q1", "q1_message"), e2 -> {
                eventBusSend(buildEnqueueOperation("q2", "q2_message"), e3 -> {
                    eventBusSend(buildEnqueueOperation("q3", "q3_message"), e4 -> {
                        given()
                                .queryParam(BULK_DELETE)
                                .body("{\"queues\": [\"a\",\"b\", 123456]}")
                                .when().post("/queuing/queues/")
                                .then().assertThat()
                                .statusCode(400)
                                .body(containsString("Queues must be string values"));

                        assertQueuesCount(context, 3);
                        assertQueueItemsCount(context, "q1", 2);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 1);

                        given()
                                .queryParam(BULK_DELETE)
                                .body("{\"zzz\": [\"a\",\"b\",\"c\"]}")
                                .when().post("/queuing/queues/")
                                .then().assertThat()
                                .statusCode(400)
                                .body(containsString("no array called 'queues' found"));

                        assertQueuesCount(context, 3);
                        assertQueueItemsCount(context, "q1", 2);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 1);

                        given()
                                .queryParam(BULK_DELETE)
                                .body("{\"zzz\": [\"a\",\"b\",\"c\"]")
                                .when().post("/queuing/queues/")
                                .then().assertThat()
                                .statusCode(400)
                                .body(containsString("failed to parse request payload"));

                        assertQueuesCount(context, 3);
                        assertQueueItemsCount(context, "q1", 2);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 1);

                        given()
                                .queryParam(BULK_DELETE)
                                .body("{\"queues\": []}")
                                .when().post("/queuing/queues/")
                                .then().assertThat()
                                .statusCode(400)
                                .body(containsString("array 'queues' is not allowed to be empty"));

                        assertQueuesCount(context, 3);
                        assertQueueItemsCount(context, "q1", 2);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 1);

                        given()
                                .queryParam(BULK_DELETE)
                                .body("{\"queues\": [\"q1\",\"q3\"]}")
                                .when().post("/queuing/queues/")
                                .then().assertThat()
                                .statusCode(200)
                                .body("deleted", equalTo(2));

                        assertQueuesCount(context, 1);
                        assertQueueItemsCount(context, "q1", 0);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 0);

                        given()
                                .queryParam(BULK_DELETE)
                                .body("{\"queues\": [\"q1\",\"q3\"]}")
                                .when().post("/queuing/queues/")
                                .then().assertThat()
                                .statusCode(200)
                                .body("deleted", equalTo(0));

                        assertQueuesCount(context, 1);
                        assertQueueItemsCount(context, "q1", 0);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 0);

                        given()
                                .queryParam(BULK_DELETE)
                                .body("{\"queues\": [1111]}")
                                .when().post("/queuing/queues/")
                                .then().assertThat()
                                .statusCode(400)
                                .body(containsString("Queues must be string values"));

                        assertQueuesCount(context, 1);
                        assertQueueItemsCount(context, "q1", 0);
                        assertQueueItemsCount(context, "q2", 1);
                        assertQueueItemsCount(context, "q3", 0);

                        async.complete();
                    });
                });
            });
        });

        async.awaitSuccess();
    }


    @Test
    public void getAllLocksWhenNoLocksPresent(TestContext context) {
        Async async = context.async();
        flushAll();
        when().get("/queuing/locks/")
                .then().assertThat()
                .statusCode(200)
                .body(LOCKS, empty());
        async.complete();
    }

    @Test
    public void getAllLocks(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildPutLockOperation("queue1", "someuser"), message -> {
            eventBusSend(buildPutLockOperation("queue2", "someuser"), message2 -> {
                when().get("/queuing/locks/")
                        .then().assertThat()
                        .statusCode(200)
                        .body(LOCKS, hasItems("queue1", "queue2"));
                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void getAllLocksFiltered(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildPutLockOperation("aaa", "someuser"), message -> {
            eventBusSend(buildPutLockOperation("aab", "someuser"), message2 -> {
                eventBusSend(buildPutLockOperation("abc", "someuser"), message3 -> {

                    given().param("filter", "^a$")
                            .when().get("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(LOCKS, is(emptyCollectionOf(String.class)));

                    given().param("filter", "a")
                            .when().get("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(LOCKS, hasItems("aaa", "aab", "abc"));

                    given().param("filter", "ab")
                            .when().get("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(LOCKS, hasItems("aab", "abc"));

                    given().param("filter", "c")
                            .when().get("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(LOCKS, hasItems("abc"));

                    async.complete();

                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void getAllLocksFilteredInvalidFilterPattern(TestContext context) {
        flushAll();
        given().param("filter", "abc(.*")
                .when().get("/queuing/locks/")
                .then().assertThat()
                .statusCode(400);
    }

    @Test
    public void bulkDeleteLocks(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildPutLockOperation("queue1", "someuser"), m1 -> {
            eventBusSend(buildPutLockOperation("queue2", "someuser"), m2 -> {
                eventBusSend(buildPutLockOperation("queue{3}", "someuser"), m3 -> {
                    given()
                            .queryParam(BULK_DELETE)
                            .body("{\"locks\": [\"a\",\"b\",123456]}")
                            .when().post("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200); // not 400 any more vertx 4 convert any number in request into String
                    //  .body(containsString("Locks must be string values"));

                    assertLockExists(context, "queue1");
                    assertLockExists(context, "queue2");
                    assertLockExists(context, "queue{3}");

                    given()
                            .queryParam(BULK_DELETE)
                            .body("{\"zzz\": [\"a\",\"b\",\"c\"]}")
                            .when().post("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(400)
                            .body(containsString("no array called 'locks' found"));

                    assertLockExists(context, "queue1");
                    assertLockExists(context, "queue2");
                    assertLockExists(context, "queue{3}");

                    given()
                            .queryParam(BULK_DELETE)
                            .body("{\"zzz\": [\"a\",\"b\",\"c\"]")
                            .when().post("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(400)
                            .body(containsString("failed to parse request payload"));

                    assertLockExists(context, "queue1");
                    assertLockExists(context, "queue2");
                    assertLockExists(context, "queue{3}");

                    given()
                            .queryParam(BULK_DELETE)
                            .body("{\"locks\": []}")
                            .when().post("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(400)
                            .body(containsString("array 'locks' is not allowed to be empty"));

                    assertLockExists(context, "queue1");
                    assertLockExists(context, "queue2");
                    assertLockExists(context, "queue{3}");

                    given()
                            .queryParam(BULK_DELETE)
                            .body("{\"locks\": [\"queue1\",\"queue{3}\"]}")
                            .when().post("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("deleted", equalTo(2));

                    assertLockDoesNotExist(context, "queue1");
                    assertLockExists(context, "queue2");
                    assertLockDoesNotExist(context, "queue{3}");

                    given()
                            .queryParam(BULK_DELETE)
                            .body("{\"locks\": [\"queue1\",\"queue{3}\"]}")
                            .when().post("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("deleted", equalTo(0));

                    assertLockDoesNotExist(context, "queue1");
                    assertLockExists(context, "queue2");
                    assertLockDoesNotExist(context, "queue{3}");

                    given()
                            .queryParam(BULK_DELETE)
                            .body("{\"locks\": [\"queue2\"]}")
                            .when().post("/queuing/locks/")
                            .then().assertThat()
                            .statusCode(200)
                            .body("deleted", equalTo(1));

                    assertLockDoesNotExist(context, "queue1");
                    assertLockDoesNotExist(context, "queue2");
                    assertLockDoesNotExist(context, "queue{3}");

                    async.complete();
                });
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void deleteAllLocks(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildPutLockOperation("queue1", "someuser"), message -> {
            eventBusSend(buildPutLockOperation("queue{2}", "someuser"), message2 -> {
                when().delete("/queuing/locks/")
                        .then().assertThat()
                        .statusCode(200)
                        .body("deleted", equalTo(2));

                //delete all locks again
                when().delete("/queuing/locks/")
                        .then().assertThat()
                        .statusCode(200)
                        .body("deleted", equalTo(0));

                async.complete();
            });
        });
        async.awaitSuccess();
    }

    @Test
    public void getSingleLockNotExisting(TestContext context) {
        Async async = context.async();
        flushAll();
        when().get("/queuing/locks/notExisiting_" + System.currentTimeMillis())
                .then().assertThat()
                .statusCode(404)
                .body(containsString("No such lock"));
        async.complete();
    }

    @Test
    public void getSingleLock(TestContext context) {
        Async async = context.async();
        flushAll();
        Long ts = System.currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;
        eventBusSend(buildPutLockOperation(lock, requestedBy), message -> {
            when().get("/queuing/locks/" + lock)
                    .then().assertThat()
                    .statusCode(200)
                    .body(
                            REQUESTED_BY, equalTo(requestedBy),
                            TIMESTAMP, greaterThanOrEqualTo(ts)
                    );
            async.complete();
        });
        async.awaitSuccess();
    }

    @Test
    public void getSingleLockEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        Long ts = System.currentTimeMillis();
        String lock = "myLock%7Bwith_curly_brackets%7D";
        String lockDecoded = "myLock{with_curly_brackets}";
        String requestedBy = "someuser_" + ts;
        eventBusSend(buildPutLockOperation(lockDecoded, requestedBy), message -> {
            given().urlEncodingEnabled(false).when().get("/queuing/locks/" + lock)
                    .then().assertThat()
                    .statusCode(200)
                    .body(
                            REQUESTED_BY, equalTo(requestedBy),
                            TIMESTAMP, greaterThanOrEqualTo(ts)
                    );
            async.complete();
        });
        async.awaitSuccess();
    }

    @Test
    public void addLock(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;

        assertLockDoesNotExist(context, lock);

        given()
                .header("x-rp-usr", requestedBy)
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        assertLockExists(context, lock);
        assertLockContent(context, lock, requestedBy);

        async.complete();
    }

    @Test
    public void addLockEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = currentTimeMillis();
        String lock = "myLock%7Bwith_curly_brackets%7D";
        String lockDecoded = "myLock{with_curly_brackets}";
        String requestedBy = "someuser_" + ts;

        assertLockDoesNotExist(context, lock);
        assertLockDoesNotExist(context, lockDecoded);

        given()
                .urlEncodingEnabled(false)
                .header("x-rp-usr", requestedBy)
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        assertLockExists(context, lockDecoded);
        assertLockContent(context, lockDecoded, requestedBy);

        async.complete();
    }

    @Test
    public void addLockWrongUserHeader(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;

        assertLockDoesNotExist(context, lock);

        given()
                .header("wrong-user-header", requestedBy)
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        assertLockExists(context, lock);
        assertLockContent(context, lock, "Unknown");

        async.complete();
    }

    @Test
    public void addLockNoUserHeader(TestContext context) {
        Async async = context.async();
        flushAll();
        String lock = "myLock_" + currentTimeMillis();

        assertLockDoesNotExist(context, lock);

        given()
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        assertLockExists(context, lock);
        assertLockContent(context, lock, "Unknown");

        async.complete();
    }

    @Test
    public void bulkPutLocks(TestContext context) {
        Async async = context.async();
        flushAll();

        // check no locks exist yet
        when().get("/queuing/locks/")
                .then().assertThat()
                .statusCode(200)
                .body(LOCKS, empty());

        given()
                .body("{\"locks\": [\"queue1\",\"queue2\",\"queue3\", 123456]}")
                .when().post("/queuing/locks/")
                .then().assertThat()
                .statusCode(400)
                .body(containsString("Locks must be string values"));

        assertLockDoesNotExist(context, "queue1");
        assertLockDoesNotExist(context, "queue2");
        assertLockDoesNotExist(context, "queue3");

        given()
                .body("{\"zzz\": [\"queue1\",\"queue2\",\"queue3\"]}")
                .when().post("/queuing/locks/")
                .then().assertThat()
                .statusCode(400)
                .body(containsString("no array called 'locks' found"));

        assertLockDoesNotExist(context, "queue1");
        assertLockDoesNotExist(context, "queue2");
        assertLockDoesNotExist(context, "queue3");

        given()
                .body("{\"zzz\": [\"queue1\",\"queue2\",\"queue3\"]")
                .when().post("/queuing/locks/")
                .then().assertThat()
                .statusCode(400)
                .body(containsString("failed to parse request payload"));

        assertLockDoesNotExist(context, "queue1");
        assertLockDoesNotExist(context, "queue2");
        assertLockDoesNotExist(context, "queue3");

        given()
                .body("{\"locks\": []}")
                .when().post("/queuing/locks/")
                .then().assertThat()
                .statusCode(400)
                .body(containsString("array 'locks' is not allowed to be empty"));

        Long ts = System.currentTimeMillis();

        given()
                .body("{\"locks\": [\"queue1\",\"queue2\",\"queue{3}\"]}")
                .when().post("/queuing/locks/")
                .then().assertThat()
                .statusCode(200);

        assertLockExists(context, "queue1");
        assertLockExists(context, "queue2");
        assertLockExists(context, "queue{3}");

        when().get("/queuing/locks/")
                .then().assertThat()
                .statusCode(200)
                .body(LOCKS, hasItems("queue1", "queue2", "queue{3}"));

        when().get("/queuing/locks/queue1")
                .then().assertThat()
                .statusCode(200)
                .body(
                        REQUESTED_BY, equalTo("Unknown"),
                        TIMESTAMP, greaterThanOrEqualTo(ts)
                );

        given().urlEncodingEnabled(false).when().get("/queuing/locks/queue%7B3%7D")
                .then().assertThat()
                .statusCode(200)
                .body(
                        REQUESTED_BY, equalTo("Unknown"),
                        TIMESTAMP, greaterThanOrEqualTo(ts)
                );

        Long ts2 = System.currentTimeMillis();

        given().header("x-rp-usr", "geronimo")
                .body("{\"locks\": [\"queue4\",\"queue5\",\"queue6\"]}")
                .when().post("/queuing/locks/")
                .then().assertThat()
                .statusCode(200);

        assertLockExists(context, "queue1");
        assertLockExists(context, "queue2");
        assertLockExists(context, "queue{3}");
        assertLockExists(context, "queue4");
        assertLockExists(context, "queue5");
        assertLockExists(context, "queue6");

        when().get("/queuing/locks/")
                .then().assertThat()
                .statusCode(200)
                .body(LOCKS, hasItems("queue1", "queue2", "queue{3}", "queue4", "queue5", "queue6"));

        when().get("/queuing/locks/queue5")
                .then().assertThat()
                .statusCode(200)
                .body(
                        REQUESTED_BY, equalTo("geronimo"),
                        TIMESTAMP, greaterThanOrEqualTo(ts2)
                );

        Long ts3 = System.currentTimeMillis();

        // overwrite existing lock
        given().header("x-rp-usr", "winnetou")
                .body("{\"locks\": [\"queue5\"]}")
                .when().post("/queuing/locks/")
                .then().assertThat()
                .statusCode(200);

        when().get("/queuing/locks/queue5")
                .then().assertThat()
                .statusCode(200)
                .body(
                        REQUESTED_BY, equalTo("winnetou"),
                        TIMESTAMP, greaterThanOrEqualTo(ts3)
                );

        async.complete();

        async.awaitSuccess();
    }


    @Test
    public void deleteSingleLockNotExisting(TestContext context) {
        Async async = context.async();
        flushAll();
        when().delete("/queuing/locks/notExisiting_" + System.currentTimeMillis()).then().assertThat().statusCode(200);
        async.complete();
    }

    @Test
    public void deleteSingleLock(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = System.currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;
        eventBusSend(buildPutLockOperation(lock, requestedBy), message -> {

            assertLockExists(context, lock);

            when().delete("/queuing/locks/" + lock)
                    .then().assertThat()
                    .statusCode(200);

            assertLockDoesNotExist(context, lock);

            async.complete();
        });
        async.awaitSuccess();
    }

    @Test
    public void deleteSingleLockEncoded(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = System.currentTimeMillis();
        String lock = "myLock%7Bwith_curly_brackets%7D";
        String lockDecoded = "myLock{with_curly_brackets}";
        String requestedBy = "someuser_" + ts;
        eventBusSend(buildPutLockOperation(lockDecoded, requestedBy), message -> {

            assertLockExists(context, lockDecoded);

            given().urlEncodingEnabled(false).when().delete("/queuing/locks/" + lock)
                    .then().assertThat()
                    .statusCode(200);

            assertLockDoesNotExist(context, lockDecoded);

            async.complete();
        });
        async.awaitSuccess();
    }

    @Test
    public void getMonitorInformation(TestContext context) {
        Async async = context.async();
        flushAll();

        when().get("/queuing/monitor").then().assertThat().statusCode(200)
                .body("queues", empty());

        // prepare
        eventBusSend(buildEnqueueOperation("queue_1", "item1_1"), null);
        eventBusSend(buildEnqueueOperation("queue_1", "item1_2"), null);
        eventBusSend(buildEnqueueOperation("queue_1", "item1_3"), null);

        eventBusSend(buildEnqueueOperation("queue_2", "item2_1"), null);
        eventBusSend(buildEnqueueOperation("queue_2", "item2_2"), null);

        eventBusSend(buildEnqueueOperation("queue_3", "item3_1"), null);

        // lock queue
        given().body("{}").when().put("/queuing/locks/queue_3").then().assertThat().statusCode(200);
        when().delete("/queuing/queues/queue_3/0").then().assertThat().statusCode(200);

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

        when().get("/queuing/monitor").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedNoEmptyQueuesNoLimit).toString()));

        String expectedWithEmptyQueuesNoLimit = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_3\",\n" +
                "      \"size\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor?emptyQueues").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedWithEmptyQueuesNoLimit).toString()));

        String expectedNoEmptyQueuesAndLimit3 = "{\n" +
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

        when().get("/queuing/monitor?limit=3").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedNoEmptyQueuesAndLimit3).toString()));

        String expectedWithEmptyQueuesAndLimit3 = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_3\",\n" +
                "      \"size\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor?limit=3&emptyQueues").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedWithEmptyQueuesAndLimit3).toString()));

        String expectedWithEmptyQueuesAndInvalidLimit = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_3\",\n" +
                "      \"size\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor?limit=xx99xx&emptyQueues").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedWithEmptyQueuesAndInvalidLimit).toString()));

        async.complete();
    }

    @Test
    public void getStatisticsEmpty(TestContext context) {
        flushAll();
        assertQueueState(context, null, 0, null,
                null, null, null, null,
                null);
    }

    @Test
    public void getStatisticsFailures(TestContext context) {
        flushAll();

        // Check normal send with missing message consumer -> 1 failure immediately
        Async async1 = context.async();
        eventBusSend(buildEnqueueOperation("stat_a", "item_a_1"), handler -> {
            assertQueueState(context, null, 1, 0,
                    "stat_a", 1, 1, null,
                    null);
            async1.complete();
        });
        async1.awaitSuccess();
        Async async2 = context.async();
        eventBusSend(buildEnqueueOperation("stat_a", "item_a_2"), handler -> {
            assertQueueState(context, null, 1, 0,
                    "stat_a", 2, 2, null,
                    null);
            async2.complete();
        });
        async2.awaitSuccess();
    }

    @Test(timeout = 10000L)
    public void getStatisticsSlowDown(TestContext context) {
        Async async = context.async();
        flushAll();

        // Test increasing slowDown Time within 5 seconds
        eventBusSend(buildEnqueueOperation("stat_b", "item_b_1"), handler -> {
            assertQueueState(context, null, 1, 0,
                    "stat_b", 1, 1, 1,
                    null);
            async.complete();
        });
        async.awaitSuccess(8000);
    }

    @Test(timeout = 10000L)
    public void getStatisticsBackpressure(TestContext context) {
        flushAll();

        // Test increasing backPressure Time
        // see the QueueConfiguration for stat_* queues in the @Before section
        Async async1 = context.async();
        eventBusSend(buildEnqueueOperation("stat_c", "item_c_1"), handler -> {
            assertQueueState(context, null, 1, 0,
                    "stat_c", 1, null, null,
                    0);
            async1.complete();
        });
        async1.awaitSuccess();

        Async async2 = context.async();
        eventBusSend(buildEnqueueOperation("stat_c", "item_c_2"), h1 -> {
            assertQueueState(context, null, 1, 0,
                    "stat_c", 2, null, null,
                    5);
            async2.complete();
        });
        async2.awaitSuccess();

        Async async3 = context.async();
        eventBusSend(buildEnqueueOperation("stat_c", "item_c_3"), h1 -> {
            assertQueueState(context, null, 1, 0,
                    "stat_c", 3, null, null,
                    10);
            async3.complete();
        });
        async3.awaitSuccess(8000);
    }

    @Test
    public void getStatisticsFilter(TestContext context) {
        Async async = context.async();
        flushAll();
        // Test retrieve statistics with queue filter
        final int loopLimit = 10;
        AtomicInteger loopCtr = new AtomicInteger();
        for (int i = 0; i < loopLimit; i++) {
            eventBusSend(buildEnqueueOperation("stat_" + i, "item_1_stat_" + i), handler -> {
                if (loopCtr.getAndIncrement() >= loopLimit - 1) {
                    assertQueueState(context, "stat_[1-5]", 5, null,
                            null, null, null, null,
                            null);
                    assertQueueState(context, "stat_1", 1, 0,
                            "stat_1", null, null, null,
                            null);
                    async.complete();
                }
            });
        }
        async.awaitSuccess();
    }

    @Test
    public void getQueueSpeedEmptyFilter(TestContext context) {
        MessageConsumer consumer = registerQueueEventOkConsumer();
        flushAll();
        Async async = context.async(3);
        eventBusSend(buildEnqueueOperation("speed_a", "item_a_1"), handler -> async.countDown());
        eventBusSend(buildEnqueueOperation("speed_a", "item_a_2"), handler -> async.countDown());
        eventBusSend(buildEnqueueOperation("speed_b", "item_b_1"), handler -> async.countDown());
        async.awaitSuccess();
        // If no filter is given, this would result in the speed over all queues known.
        assertQueueSpeed(context, "", 3);
        consumer.unregister();
    }

    @Test
    public void getQueueSpeedNoMatchingFilter(TestContext context) {
        MessageConsumer consumer = registerQueueEventOkConsumer();
        flushAll();
        Async async = context.async();
        eventBusSend(buildEnqueueOperation("speed_a", "item_a_1"), handler -> async.complete());
        async.awaitSuccess();
        // we should not get any speed for this other queue and end up in timeout therefore
        assertQueueSpeed(context, "speed_b.*", 0);
        consumer.unregister();
    }

    @Test
    public void getQueueSpeedMatchingFilter(TestContext context) {
        MessageConsumer consumer = registerQueueEventOkConsumer();
        flushAll();
        Async async = context.async(2);
        eventBusSend(buildEnqueueOperation("speed_a", "item_a_1"), handler -> async.countDown());
        eventBusSend(buildEnqueueOperation("speed_a", "item_a_2"), handler -> async.countDown());
        async.awaitSuccess();
        // two messages must be captured
        assertQueueSpeed(context, "speed_a.*", 2);
        consumer.unregister();
    }

    @Test
    public void getQueueSpeedMatchingPartlyFilter(TestContext context) {
        MessageConsumer consumer = registerQueueEventOkConsumer();
        flushAll();
        Async async = context.async(2);
        eventBusSend(buildEnqueueOperation("speed_a", "item_a_1"), handler -> async.countDown());
        eventBusSend(buildEnqueueOperation("speed_b", "item_b_1"), handler -> async.countDown());
        async.awaitSuccess();
        assertQueueSpeed(context, "speed_a.*", 1);
        consumer.unregister();
    }

    /**
     * Helper method to wait for a particular queue state. Parameters provided with null (except
     * context) will not be taken into account for the queue state comparison
     *
     * @param context               The Test context to be used
     * @param queuesSize            The expected size of the returned statistics queues
     * @param element               The element in the retrieved statistics queue array to be
     *                              checked against
     * @param queueName             The name of the particular expected queue at position=element
     * @param queueSize             The size of the particular expected queue at position=element
     * @param queueFailures         The failures of the particular expected queue at
     *                              position=element
     * @param queueSlowdownTime     The slowDownTime of the particular expected queue at
     *                              position=element
     * @param queueBackpressureTime The backpressureTime of the particular expected queue at
     *                              position=element
     * @oaram filter                The filter to be applied to the statistic queues request
     */
    private void assertQueueState(@NotNull TestContext context,
                                  @Nullable String filter,
                                  @Nullable Integer queuesSize,
                                  @Nullable Integer element,
                                  @Nullable String queueName,
                                  @Nullable Integer queueSize,
                                  @Nullable Integer queueFailures,
                                  @Nullable Integer queueSlowdownTime,
                                  @Nullable Integer queueBackpressureTime) {
        long maxWaitTime = System.currentTimeMillis() + 5000;
        while (true) {
            StringBuilder failureState = new StringBuilder("Failures");
            String url = "/queuing/statistics";
            if (filter != null && !filter.isEmpty()) {
                url = url + "?filter=" + filter;
            }
            ArrayList<HashMap<String, Object>> response = when().get(url)
                    .then()
                    .assertThat().statusCode(200)
                    .extract().path("queues");
            boolean success = true;
            // check the number of queues statistics we have
            if (queuesSize != null &&
                    response.size() != queuesSize) {
                success = false;
                failureState.append("/queuesSize:" + response.size());
            }
            if (element != null) {
                // check the queue statistic element at the given position
                //
                // check if the queue name at the position is correct
                if (queueName != null) {
                    String name = (String) response.get(element).get("name");
                    if (!name.equals(queueName)) {
                        success = false;
                        failureState.append("/queueName:" + name);
                    }
                }
                // check the queue size
                if (queueSize != null) {
                    Integer size = (Integer) response.get(element).get("size");
                    if (!queueSize.equals(size)) {
                        success = false;
                        failureState.append("/queueSize:" + size);
                    }
                }
                // check the accumulated queue failures
                if (queueFailures != null) {
                    Integer failures = (Integer) response.get(element).get("failures");
                    if (!queueFailures.equals(failures)) {
                        success = false;
                        failureState.append("/queueFailures:" + failures);
                    }
                }
                // check the queue slowDown time
                if (queueSlowdownTime != null) {
                    Integer slowDownTime = (Integer) response.get(element).get("slowdownTime");
                    if (!queueSlowdownTime.equals(slowDownTime)) {
                        success = false;
                        failureState.append("/slowDownTime:" + slowDownTime);
                    }
                }
                // check the queue backpressure time
                if (queueBackpressureTime != null) {
                    Integer backpressureTime = (Integer) response.get(element)
                            .get("backpressureTime");
                    if (!queueBackpressureTime.equals(backpressureTime)) {
                        success = false;
                        failureState.append("/backpressureTime:" + backpressureTime);
                    }
                }
            }
            if (System.currentTimeMillis() > maxWaitTime) {
                context.assertTrue(success, failureState.toString());
            } else if (success) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException iex) {
                // ignore
            }
        }
    }


    /**
     * Helper method to wait for a particular speed value for the given queue filter
     *
     * @param filter The queue filter name (regex) or empty if all
     * @param speed  The expected speed
     * @return true if the expectation was fulfilled within max 5 seconds, else false
     */
    private void assertQueueSpeed(@NotNull TestContext context,
                                  @Nullable String filter, int speed) {
        String url = "/queuing/speed";
        if (filter != null && !filter.isEmpty()) {
            url = url + "?filter=" + filter;
        }
        long maxWaitTime = System.currentTimeMillis() + 5000;
        while (true) {
            boolean success = true;
            Integer response = given().when().get(url)
                    .then().assertThat()
                    .statusCode(200)
                    .extract().path("speed");
            if (response == null || !response.equals(speed)) {
                success = false;
            }
            if (System.currentTimeMillis() > maxWaitTime) {
                context.assertTrue(success, "speed failure");
            } else if (success) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException iex) {
                // ignore
            }
        }
    }


    @Test(timeout = 15000L)
    @Ignore  // applicable only on local developers environment for performance comparisons
    public void testPerformance(TestContext context) throws Exception {
        Async async = context.async();
        ArrayList<HashMap<String, Object>> response;
        flushAll();

        long statisticsReferenceTime = 0;
        long monitoringReferenceTime = 0;

        System.out.println("----------------------- WARMING UP PHASE ----------------------------");
        {
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/statistics").then().assertThat().statusCode(200)
                    .extract().path("queues");
            assert (response.size() == 0);
            long statisticsTime = System.currentTimeMillis() - timestamp;
            System.out.println("Statistics initial time:" + statisticsTime);
        }

        {   // note: we do it a second time as it seems to be some optimising/caching applied
            // on first time within Redis. Second one is much faster.
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/statistics").then().assertThat()
                    .statusCode(200).extract().path("queues");
            assert (response.size() == 0);
            statisticsReferenceTime = System.currentTimeMillis() - timestamp;
            System.out.println("Statistics reference time=" + statisticsReferenceTime);
        }

        {
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/monitor").then().assertThat()
                    .statusCode(200).extract().path("queues");
            assert (response.size() == 0);
            long monitorTime = System.currentTimeMillis() - timestamp;
            System.out.println("Monitoring empty initial time=" + monitorTime);
        }

        {
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/monitor").then().assertThat()
                    .statusCode(200).extract().path("queues");
            assert (response.size() == 0);
            monitoringReferenceTime = System.currentTimeMillis() - timestamp;
            System.out.println("Monitoring reference time=" + monitoringReferenceTime);
        }

        System.out.println("---------------------- EVENT CREATION PHASE -------------------------");

        { // feed the system with a bunch of messages for further tests
            long timestamp = System.currentTimeMillis();
            final AtomicInteger ctr = new AtomicInteger(0);
            while (ctr.get() < 1000) {
                eventBusSend(buildEnqueueOperation("queue_" + ctr, "item"),
                        emptyHandler -> {
                            ctr.incrementAndGet();
                            synchronized (ctr) {
                                ctr.notifyAll();
                            }
                        });
                synchronized (ctr) {
                    ctr.wait();
                }
            }
            long usedTime = System.currentTimeMillis() - timestamp;
            System.out.println("Event creation time=" + usedTime);
        }

        System.out.println("---------------------- MONITORING CHECK PHASE -----------------------");

        {   // first run to warm up the system
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/monitor").then().assertThat()
                    .statusCode(200).extract().path("queues");
            assert (response.size() == 1000);
            long monitorTime = System.currentTimeMillis() - timestamp;
            System.out.println("Monitoring initial time=" + monitorTime);
        }

        {   // redo a second time in order to have any caching/optimizing stuff in place
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/monitor").then().assertThat()
                    .statusCode(200).extract().path("queues");
            assert (response.size() == 1000);
            long monitorTime = System.currentTimeMillis() - timestamp;
            System.out.println("Monitoring time=" + monitorTime);
            // assume that the used time is smaller than  xx times the reference times
            assert (monitorTime < 20 * monitoringReferenceTime);
        }

        System.out.println("-------------------- STATISTICS CHECK PHASE -------------------------");

        {   // first run to warm up the system
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/statistics").then().assertThat()
                    .statusCode(200).extract().path("queues");
            assert (response.size() == 1000);
            long statisticsTime = System.currentTimeMillis() - timestamp;
            System.out.println("Statistics initial time=" + statisticsTime);
        }

        {   // redo a second time in order to have any caching/optimizing stuff in place
            long timestamp = System.currentTimeMillis();
            response = given().when().get("/queuing/statistics").then().assertThat()
                    .statusCode(200).extract().path("queues");
            assert (response.size() == 1000);
            long statisticsTime = System.currentTimeMillis() - timestamp;
            System.out.println("Statistics time=" + statisticsTime);
            // assume that the used time is smaller than  xx times the reference times
            assert (statisticsTime < 20 * statisticsReferenceTime);
        }

        System.out.println("------------------ PERFORMANCE CHECKS COMPLETED ---------------------");
        async.complete();
        flushAll();
    }
}
