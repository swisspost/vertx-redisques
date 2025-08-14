package org.swisspush.redisques.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.*;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.lock.Lock;
import org.swisspush.redisques.util.*;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.mockito.Mockito.when;
import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Tests for {@link MetricsCollector} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class MetricsCollectorTest extends AbstractTestCase {

    private RedisQues redisQues;
    private MeterRegistry meterRegistry;
    private MetricsCollector metricsCollector;
    private Lock lock;

    private final String metricsIdentifier = "foo";

    @Rule
    public Timeout rule = Timeout.seconds(10);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();

        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .httpRequestHandlerEnabled(true)
                .redisMonitoringEnabled(false)
                .micrometerMetricsEnabled(true)
                .micrometerMetricsIdentifier(metricsIdentifier)
                .metricRefreshPeriod(999999) // use a large periodically refresh because it may interferes the test
                .refreshPeriod(2)
                .publishMetricsAddress("my-metrics-eb-address")
                .metricStorageName("foobar")
                .memoryUsageLimitPercent(80)
                .redisReadyCheckIntervalMs(2000)
                .queueConfigurations(Collections.singletonList(new QueueConfiguration()
                        .withPattern("queue.*")
                        .withRetryIntervals(2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52))
                )
                .build()
                .asJsonObject();

        lock = Mockito.mock(Lock.class);
        meterRegistry = new SimpleMeterRegistry();

        redisQues = RedisQues.builder()
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(vertx, config))
                .withMeterRegistry(meterRegistry)
                .build();

        metricsCollector = new MetricsCollector(vertx, redisQues.getUid(), "redisques", "foo",
                meterRegistry, lock, 10);

        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - {} was successful.", redisQues.getClass().getSimpleName());
            jedis = new Jedis("localhost", 6379, 5000);
        }));
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testUpdateMyQueuesStateCount(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));
            eventBusSend(buildEnqueueOperation("queueEnqueue2", "helloEnqueue2"), message1 -> {
                context.assertEquals(OK, message1.result().body().getString(STATUS));
                eventBusSend(buildEnqueueOperation("queueEnqueue3", "helloEnqueue3"), message2 -> {
                    context.assertEquals(OK, message2.result().body().getString(STATUS));
                    try {
                        // wait few seconds.
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    Mockito.when(lock.acquireLock(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong())).thenReturn(Future.succeededFuture(true));
                    metricsCollector.updateMyQueuesStateCount().onComplete(event -> {
                        context.assertEquals(3.0, getQueueReadySizeGauge().value());
                        context.assertEquals(0.0, getQueueConsumingSizeGauge().value());
                        async.complete();
                    });
                });
            });
        });
    }

    @Test
    public void testActiveQueueCountsAndMaxQueueSize(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

        // first queue having 1 item
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));

            // first queue having 3 items
            eventBusSend(buildEnqueueOperation("queueEnqueue2", "testItem1"), messageEnqueue -> {
                context.assertEquals(OK, messageEnqueue.result().body().getString(STATUS));
                eventBusSend(buildEnqueueOperation("queueEnqueue2", "testItem2"), messageEnqueue2 -> {
                    context.assertEquals(OK, messageEnqueue2.result().body().getString(STATUS));

                    eventBusSend(buildEnqueueOperation("queueEnqueue2", "testItem3"), messageEnqueue3 -> {
                        context.assertEquals(OK, messageEnqueue3.result().body().getString(STATUS));

                        Mockito.when(lock.acquireLock(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong())).thenReturn(Future.succeededFuture(true));
                        metricsCollector.updateActiveQueuesCount().onComplete(event -> {
                            context.assertEquals(2.0, getActiveQueuesCountGauge().value());

                            metricsCollector.updateMaxQueueSize().onComplete(maxQueueSizeEvent -> {
                                context.assertEquals(3.0, getMaxQueueSizeGauge().value());
                                async.complete();
                            });
                        });
                    });
                });
            });
        });
    }

    @Test
    public void testNoUpdatesWhenLockCouldNotBeAcquired(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));

            Mockito.when(lock.acquireLock(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong())).thenReturn(Future.succeededFuture(true));
            metricsCollector.updateActiveQueuesCount().onComplete(event -> {
                context.assertEquals(1.0, getActiveQueuesCountGauge().value());

                eventBusSend(buildEnqueueOperation("queueEnqueue2", "helloEnqueue2"), message2 -> {
                    context.assertEquals(OK, message2.result().body().getString(STATUS));

                    Mockito.when(lock.acquireLock(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong())).thenReturn(Future.succeededFuture(false));
                    metricsCollector.updateActiveQueuesCount().onComplete(event2 -> {
                        context.assertEquals(1.0, getActiveQueuesCountGauge().value(), "Queue count should still be 1");
                        async.complete();
                    });
                });
            });

        });
    }

    @Test
    public void testNoUpdatesWhenLockAcquireFailed(TestContext context) {
        Async async = context.async();
        flushAll();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            context.assertEquals(OK, message.result().body().getString(STATUS));

            Mockito.when(lock.acquireLock(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong())).thenReturn(Future.succeededFuture(true));
            metricsCollector.updateActiveQueuesCount().onComplete(event -> {
                context.assertEquals(1.0, getActiveQueuesCountGauge().value());

                eventBusSend(buildEnqueueOperation("queueEnqueue2", "helloEnqueue2"), message2 -> {
                    context.assertEquals(OK, message2.result().body().getString(STATUS));

                    Mockito.when(lock.acquireLock(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong())).thenReturn(Future.failedFuture("boooom"));
                    metricsCollector.updateActiveQueuesCount().onComplete(event2 -> {
                        context.assertEquals(1.0, getActiveQueuesCountGauge().value(), "Queue count should still be 1");
                        async.complete();
                    });
                });
            });

        });
    }

    private Gauge getActiveQueuesCountGauge(){
        return meterRegistry.get(MetricMeter.ACTIVE_QUEUES.getId()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).gauge();
    }

    private Gauge getMaxQueueSizeGauge(){
        return meterRegistry.get(MetricMeter.MAX_QUEUE_SIZE.getId()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).gauge();
    }

    private Gauge getQueueConsumingSizeGauge(){
        return meterRegistry.get(MetricMeter.QUEUE_STATE_CONSUMING_SIZE.getId()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).gauge();
    }

    private Gauge getQueueReadySizeGauge(){
        return meterRegistry.get(MetricMeter.QUEUE_STATE_READY_SIZE.getId()).tag(MetricTags.IDENTIFIER.getId(), metricsIdentifier).gauge();
    }
}
