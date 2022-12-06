package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.impl.types.BulkType;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link DefaultMemoryUsageProvider} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class DefaultMemoryUsageProviderTest {

    private DefaultMemoryUsageProvider memoryUsageProvider;
    private RedisAPI redisAPI;

    private Vertx vertx;

    private static final String REDIS_INFO_50_NOVALUES = "# Memory\n" +
            "used_memory_rss:688272\n" +
            "used_memory_rss_human:672.14K\n" +
            "used_memory_peak:3765568\n" +
            "used_memory_peak_human:3.59M\n" +
            "total_system_memory:3953926144\n" +
            "total_system_memory_human:3.68G\n" +
            "used_memory_lua:34816\n" +
            "used_memory_lua_human:34.00K\n" +
            "maxmemory:0\n" +
            "maxmemory_human:0B\n" +
            "maxmemory_policy:noeviction\n" +
            "mem_fragmentation_ratio:0.95\n" +
            "mem_allocator:jemalloc-3.6.0\n";
    private static final String REDIS_INFO_50_PCT = "# Memory\n" +
            "used_memory:1997802672\n" +
            "used_memory_human:1.86G\n" +
            "used_memory_rss:688272\n" +
            "used_memory_rss_human:672.14K\n" +
            "used_memory_peak:3765568\n" +
            "used_memory_peak_human:3.59M\n" +
            "total_system_memory:3953926144\n" +
            "total_system_memory_human:3.68G\n" +
            "used_memory_lua:34816\n" +
            "used_memory_lua_human:34.00K\n" +
            "maxmemory:0\n" +
            "maxmemory_human:0B\n" +
            "maxmemory_policy:noeviction\n" +
            "mem_fragmentation_ratio:0.95\n" +
            "mem_allocator:jemalloc-3.6.0\n";

    private static final String REDIS_INFO_0_PCT = "# Memory\n" +
            "used_memory:0\n" +
            "used_memory_human:0G\n" +
            "used_memory_rss:688272\n" +
            "used_memory_rss_human:672.14K\n" +
            "used_memory_peak:3765568\n" +
            "used_memory_peak_human:3.59M\n" +
            "total_system_memory:3953926144\n" +
            "total_system_memory_human:3.68G\n" +
            "used_memory_lua:34816\n" +
            "used_memory_lua_human:34.00K\n" +
            "maxmemory:0\n" +
            "maxmemory_human:0B\n" +
            "maxmemory_policy:noeviction\n" +
            "mem_fragmentation_ratio:0.95\n" +
            "mem_allocator:jemalloc-3.6.0\n";

    private static final String REDIS_INFO_100_PCT = "# Memory\n" +
            "used_memory:3953916142\n" +
            "used_memory_human:3.68G\n" +
            "used_memory_rss:688272\n" +
            "used_memory_rss_human:672.14K\n" +
            "used_memory_peak:3765568\n" +
            "used_memory_peak_human:3.59M\n" +
            "total_system_memory:3953926144\n" +
            "total_system_memory_human:3.68G\n" +
            "used_memory_lua:34816\n" +
            "used_memory_lua_human:34.00K\n" +
            "maxmemory:0\n" +
            "maxmemory_human:0B\n" +
            "maxmemory_policy:noeviction\n" +
            "mem_fragmentation_ratio:0.95\n" +
            "mem_allocator:jemalloc-3.6.0\n";

    @Before
    public void setup(TestContext context) {
        redisAPI = Mockito.mock(RedisAPI.class);
        vertx = Vertx.vertx();
    }

    @Test
    public void testCurrentMemoryUsageError(TestContext context) {
        when(redisAPI.info(eq(Collections.singletonList("memory"))))
                .thenReturn(Future.failedFuture("Booooooooooooom"));
        memoryUsageProvider = new DefaultMemoryUsageProvider(redisAPI, vertx, 2);

        context.assertFalse(memoryUsageProvider.currentMemoryUsagePercentage().isPresent());
    }

    @Test
    public void testCurrentMemoryUsageNoValues(TestContext context){
        when(redisAPI.info(eq(Collections.singletonList("memory"))))
                .thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_50_NOVALUES), false)));
        memoryUsageProvider = new DefaultMemoryUsageProvider(redisAPI, vertx, 2);

        context.assertFalse(memoryUsageProvider.currentMemoryUsagePercentage().isPresent());
    }

    @Test
    public void testCurrentMemoryUsage50Pct(TestContext context){
        when(redisAPI.info(eq(Collections.singletonList("memory"))))
                .thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_50_PCT), false)));
        memoryUsageProvider = new DefaultMemoryUsageProvider(redisAPI, vertx, 2);

        Optional<Integer> currentMemoryUsagePercentage = memoryUsageProvider.currentMemoryUsagePercentage();
        context.assertTrue(currentMemoryUsagePercentage.isPresent());
        context.assertEquals(51, currentMemoryUsagePercentage.get());
    }

    @Test
    public void testCurrentMemoryUsage0Pct(TestContext context){
        when(redisAPI.info(eq(Collections.singletonList("memory"))))
                .thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_0_PCT), false)));
        memoryUsageProvider = new DefaultMemoryUsageProvider(redisAPI, vertx, 2);

        Optional<Integer> currentMemoryUsagePercentage = memoryUsageProvider.currentMemoryUsagePercentage();
        context.assertTrue(currentMemoryUsagePercentage.isPresent());
        context.assertEquals(0, currentMemoryUsagePercentage.get());
    }

    @Test
    public void testCurrentMemoryUsage100Pct(TestContext context){
        when(redisAPI.info(eq(Collections.singletonList("memory"))))
                .thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_100_PCT), false)));
        memoryUsageProvider = new DefaultMemoryUsageProvider(redisAPI, vertx, 2);

        Optional<Integer> currentMemoryUsagePercentage = memoryUsageProvider.currentMemoryUsagePercentage();
        context.assertTrue(currentMemoryUsagePercentage.isPresent());
        context.assertEquals(100, currentMemoryUsagePercentage.get());
    }

    @Test
    public void testCurrentMemoryUsageUpdate(TestContext context){
        // simulate 100% memory usage
        when(redisAPI.info(eq(Collections.singletonList("memory"))))
                .thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_100_PCT), false)));
        memoryUsageProvider = new DefaultMemoryUsageProvider(redisAPI, vertx, 2);

        Optional<Integer> currentMemoryUsagePercentage = memoryUsageProvider.currentMemoryUsagePercentage();
        context.assertTrue(currentMemoryUsagePercentage.isPresent());
        context.assertEquals(100, currentMemoryUsagePercentage.get());

        // simulate 50% memory usage
        when(redisAPI.info(eq(Collections.singletonList("memory"))))
                .thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_50_PCT), false)));

        Awaitility.await().atMost(Duration.ofSeconds(3)).until(
                () -> memoryUsageProvider.currentMemoryUsagePercentage().get().equals(51), equalTo(true));
    }
}
