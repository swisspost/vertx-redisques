package org.swisspush.redisques.handler;

import io.restassured.RestAssured;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.*;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.util.RedisquesConfiguration;
import redis.clients.jedis.Jedis;

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
                .redisEncoding("ISO-8859-1")
                .refreshPeriod(2)
                .httpRequestHandlerEnabled(true)
                .httpRequestHandlerPort(7070)
                .build()
                .asJsonObject();

        RedisQues redisQues = new RedisQues();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //do nothing
            }
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
        testVertx.eventBus().send(getRedisquesAddress(), operation, handler);
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
                .body("any { it.key == 'redisEncoding' }", is(true))
                .body("any { it.key == 'redis-prefix' }", is(true))
                .body("any { it.key == 'address' }", is(true))
                .body("any { it.key == 'processor-address' }", is(true))
                .body("any { it.key == 'refresh-period' }", is(true))
                .body("any { it.key == 'checkInterval' }", is(true))
                .body("any { it.key == 'processorTimeout' }", is(true))
                .body("any { it.key == 'processorDelayMax' }", is(true))
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
            eventBusSend(buildPutLockOperation("queue2", "someuser"), m2-> {
                eventBusSend(buildPutLockOperation("queue{3}", "someuser"), m3-> {
            given()
                    .queryParam(BULK_DELETE)
                    .body("{\"locks\": [\"a\",\"b\",123456]}")
                    .when().post("/queuing/locks/")
                    .then().assertThat()
                    .statusCode(400)
                    .body(containsString("Locks must be string values"));

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
}
