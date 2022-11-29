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
import org.hamcrest.Matchers;
import org.junit.*;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.util.RedisquesConfiguration;
import redis.clients.jedis.Jedis;

import static io.restassured.RestAssured.when;
import static org.swisspush.redisques.util.RedisquesAPI.buildEnqueueOperation;

/**
 * Very similar to {@link RedisquesHttpRequestHandlerTest}, but explicitly disable the queue name decoding.
 */
public class DisableQueueNameEncodingTest extends AbstractTestCase {
    private Vertx testVertx;

    @Rule
    public Timeout rule = Timeout.seconds(5);

    private static boolean urlEncodingEnabled;

    @BeforeClass
    public static void beforeClass() {
        RestAssured.baseURI = "http://127.0.0.1/";
        RestAssured.port = 7070;
        // cache previous value
        urlEncodingEnabled = RestAssured.urlEncodingEnabled;
        RestAssured.urlEncodingEnabled = false;
    }

    @AfterClass
    public static void afterClass() {
        // restore previous value
        RestAssured.urlEncodingEnabled = urlEncodingEnabled;
    }

    @Before
    public void deployRedisques(TestContext context) {
        Async async = context.async();
        testVertx = Vertx.vertx();

        JsonObject config = RedisquesConfiguration.with()
            .address(getRedisquesAddress())
            .processorAddress("processor-address")
            .refreshPeriod(2)
            .httpRequestHandlerEnabled(true)
            .httpRequestHandlerPort(7070)
            .enableQueueNameDecoding(false)
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
        testVertx.eventBus().request(getRedisquesAddress(), operation, handler);
    }

    @Test
    public void disableEncodingQueueNameTest(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue+with_plus+signs", "helloEnqueue"), message -> {
            eventBusSend(buildEnqueueOperation("queue+with_plus+signs", "helloEnqueue2"), message2 -> {
                when().get("/queuing/queues/queue+with_plus+signs")
                    .then().assertThat()
                    .statusCode(200)
                    // has a potential to be very shaky, i.e. if the json response contains spaces
                    // we can't use `.body("queue+with_plus+signs", hasItems("helloEnqueue", "helloEnqueue2"))`,
                    // because restassured is interpreting the plus sign (+) in some way
                    .body(Matchers.equalTo("{\"queue+with_plus+signs\":[\"helloEnqueue\",\"helloEnqueue2\"]}"))
                ;
                async.complete();
            });
        });
        async.awaitSuccess();
    }
}
