package org.swisspush.redisques;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Tests for the {@link RedisOptions#setMaxWaitingHandlers(int)} method
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class MaxWaitingHandlersTest {

    protected static Logger log = LoggerFactory.getLogger(MaxWaitingHandlersTest.class);

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Test(timeout = 10_000L)
    public void testRedisQueueFull(TestContext should) {
        final Async test = should.async();
        final Vertx vertx = rule.vertx();

        RedisOptions options = new RedisOptions()
                .setMaxWaitingHandlers(10) // we create only a small number of allowed waiting handlers
                .addConnectionString("redis://localhost:6379");

        Redis.createClient(rule.vertx(), options)
                .connect(create -> {
                    should.assertTrue(create.succeeded());

                    final RedisConnection redis = create.result();
                    final RedisAPI redisApi = RedisAPI.api(redis);

                    AtomicInteger cnt = new AtomicInteger();
                    List<Future> futures = new ArrayList<>();
                    IntStream.range(0, 100).forEach(i -> {
                        Promise<Response> p = Promise.promise();
                        vertx.setTimer(1, timerId -> redisApi.set(Arrays.asList("foo", "bar"), p));
                        futures.add(p.future().map(res -> {
                            log.info("SUCCESS {}", cnt.incrementAndGet());
                            return null;
                        }));
                    });

                    CompositeFuture.all(futures)
                            .onFailure(f -> {
                                should.assertEquals("Redis waiting Queue is full", f.getMessage());
                                test.complete();
                            })
                            .onSuccess(r -> {
                                if (cnt.get() == 100) {
                                    should.fail("Should not succeed!");
                                }
                            });
                });
    }

    @Test(timeout = 10_000L)
    public void testRedisQueueNotFull(TestContext should) {
        final Async test = should.async();
        final Vertx vertx = rule.vertx();

        RedisOptions options = new RedisOptions()
                .setMaxWaitingHandlers(200) // we create a larger number of allowed waiting handlers
                .addConnectionString("redis://localhost:6379");

        Redis.createClient(rule.vertx(), options)
                .connect(create -> {
                    should.assertTrue(create.succeeded());

                    final RedisConnection redis = create.result();
                    final RedisAPI redisApi = RedisAPI.api(redis);

                    AtomicInteger cnt = new AtomicInteger();
                    List<Future> futures = new ArrayList<>();
                    IntStream.range(0, 100).forEach(i -> {
                        Promise<Response> p = Promise.promise();
                        vertx.setTimer(1, timerId -> redisApi.set(Arrays.asList("foo", "bar"), p));
                        futures.add(p.future().map(res -> {
                            log.info("SUCCESS {}", cnt.incrementAndGet());
                            return null;
                        }));
                    });

                    CompositeFuture.all(futures)
                            .onFailure(f -> {
                                should.fail("Should not fail!");
                                test.complete();
                            })
                            .onSuccess(r -> {
                                if (cnt.get() == 100) {
                                    test.complete();
                                } else {
                                    should.fail("Should have a count of 100!");
                                }
                            });
                });
    }

}
