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
    private RedisQues redisQuesMock;
    private MeterRegistry meterRegistry;
    private MetricsCollector metricsCollector;
    private Lock lock;

    private final String metricsIdentifier = "foo";
    private Map<String, RedisQues.QueueProcessingState> fakeMyQueue = new HashMap<>();

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
                .metricRefreshPeriod(0) // do not periodically refresh because it interferes the test
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
        redisQuesMock = Mockito.mock(RedisQues.class);




        metricsCollector = new MetricsCollector(vertx, "myuid", "redisques", "foo",
                meterRegistry, lock, 10, fakeMyQueue);

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
        for (int i=0; i <10; i++){
            RedisQues.QueueProcessingState queueProcessingState1 = new RedisQues.QueueProcessingState(RedisQues.QueueState.READY, i);
            fakeMyQueue.put("queue-1" + i, queueProcessingState1);
            if ((i % 2) == 1) {
                RedisQues.QueueProcessingState queueProcessingState2 = new RedisQues.QueueProcessingState(RedisQues.QueueState.CONSUMING, i);
                fakeMyQueue.put("queue-2" + i, queueProcessingState2);
            }
        }

        metricsCollector.updateMyQueuesStateCount();
        context.assertEquals(10.0, getQueueReadySizeGauge().value());
        context.assertEquals(5.0, getQueueConsumingSizeGauge().value());
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
