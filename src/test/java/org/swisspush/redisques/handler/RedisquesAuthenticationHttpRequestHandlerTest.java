package org.swisspush.redisques.handler;

import io.restassured.RestAssured;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.Optional;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;

/**
 * Tests for the {@link RedisquesHttpRequestHandler} class respecting authentication
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesAuthenticationHttpRequestHandlerTest extends AbstractTestCase {
    private static String deploymentId = "";
    private Vertx testVertx;
    private TestMemoryUsageProvider memoryUsageProvider;

    @Rule
    public Timeout rule = Timeout.seconds(15);

    @BeforeClass
    public static void beforeClass() {
        RestAssured.baseURI = "http://127.0.0.1/";
        RestAssured.port = 7070;
    }

    @Before
    public void setup(TestContext context) {
        testVertx = Vertx.vertx();
        memoryUsageProvider = new TestMemoryUsageProvider(Optional.of(50));
    }

    @Test
    public void testAuthenticationDisabled(TestContext context) {
        Async async = context.async();

        // Disable authentication
        JsonObject config = buildConfig(null, null, null);

        RedisQues redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(testVertx, config))
                .build();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            delay(1000);

            // Access API
            when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(200)
                    .body("queuing", hasItems("locks/", "queues/", "monitor/", "configuration/"));

            testVertx.undeploy(deploymentId, context.asyncAssertSuccess(Void -> {
                testVertx.close(context.asyncAssertSuccess());
                async.complete();
            }));

        }));
        async.awaitSuccess();
    }

    @Test
    public void testMissingUsername(TestContext context) {
        Async async = context.async();

        // Enable authentication but without username
        JsonObject config = buildConfig(true, null, "bar");

        RedisQues redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(testVertx, config))
                .build();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            delay(1000);

            // Access API
            when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(500)
                    .body(containsString("HTTP API authentication is enabled but username and/or password is missing"));

            testVertx.undeploy(deploymentId, context.asyncAssertSuccess(Void -> {
                testVertx.close(context.asyncAssertSuccess());
                async.complete();
            }));

        }));
        async.awaitSuccess();
    }

    @Test
    public void testEmptyUsername(TestContext context) {
        Async async = context.async();

        // Enable authentication but with empty username
        JsonObject config = buildConfig(true, "", "bar");

        RedisQues redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(testVertx, config))
                .build();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            delay(1000);

            // Access API
            when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(500)
                    .body(containsString("HTTP API authentication is enabled but username and/or password is missing"));

            testVertx.undeploy(deploymentId, context.asyncAssertSuccess(Void -> {
                testVertx.close(context.asyncAssertSuccess());
                async.complete();
            }));

        }));
        async.awaitSuccess();
    }

    @Test
    public void testMissingPassword(TestContext context) {
        Async async = context.async();

        // Enable authentication but without password
        JsonObject config = buildConfig(true, "foo", null);

        RedisQues redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(testVertx, config))
                .build();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            delay(1000);

            // Access API
            when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(500)
                    .body(containsString("HTTP API authentication is enabled but username and/or password is missing"));

            testVertx.undeploy(deploymentId, context.asyncAssertSuccess(Void -> {
                testVertx.close(context.asyncAssertSuccess());
                async.complete();
            }));

        }));
        async.awaitSuccess();
    }

    @Test
    public void testEmptyPassword(TestContext context) {
        Async async = context.async();

        // Enable authentication but with empty password
        JsonObject config = buildConfig(true, "foo", "");

        RedisQues redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(testVertx, config))
                .build();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            delay(1000);

            // Access API
            when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(500)
                    .body(containsString("HTTP API authentication is enabled but username and/or password is missing"));

            testVertx.undeploy(deploymentId, context.asyncAssertSuccess(Void -> {
                testVertx.close(context.asyncAssertSuccess());
                async.complete();
            }));

        }));
        async.awaitSuccess();
    }

    @Test
    public void testAuthenticationEnabled(TestContext context) {
        Async async = context.async();

        // Enable authentication
        JsonObject config = buildConfig(true, "foo", "bar");

        RedisQues redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(testVertx, config))
                .build();

        testVertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            delay(1000);

            // No Authentication added
            when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(401);

            // Wrong Authentication added
            given().auth().basic("foo", "wrong")
                    .when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(401);

            // Correct Authentication added
            given().auth().basic("foo", "bar")
                    .when().get("/queuing/")
                    .then().assertThat()
                    .statusCode(200)
                    .body("queuing", hasItems("locks/", "queues/", "monitor/", "configuration/"));

            testVertx.undeploy(deploymentId, context.asyncAssertSuccess(Void -> {
                testVertx.close(context.asyncAssertSuccess());
                async.complete();
            }));

        }));
        async.awaitSuccess();
    }

    private JsonObject buildConfig(Boolean useAuth, String username, String password) {
        RedisquesConfiguration.RedisquesConfigurationBuilder builder = RedisquesConfiguration.with()
                .address(getRedisquesAddress())
                .processorAddress("processor-address")
                .httpRequestHandlerEnabled(true)
                .httpRequestHandlerPort(7070);

        if(useAuth != null) {
            builder.httpRequestHandlerAuthenticationEnabled(useAuth)
                    .httpRequestHandlerUsername(username)
                    .httpRequestHandlerPassword(password);
        }

        return builder.build().asJsonObject();
    }
}
