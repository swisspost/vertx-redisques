package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.action.AbstractQueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;
import static org.swisspush.redisques.util.RedisquesAPI.buildAddQueueItemOperation;
import static org.swisspush.redisques.util.RedisquesAPI.buildEnqueueOperation;

public class QueueRegistryServiceTest extends AbstractTestCase {
    private RedisQues redisQues;
    private TestMemoryUsageProvider memoryUsageProvider;
    private final String metricsIdentifier = "foo";
    protected AbstractQueueAction action;
    protected RedisQuesExceptionFactory exceptionFactory;
    @Rule
    public Timeout rule = Timeout.seconds(50);

    @Before
    public void deployRedisques(TestContext context) {
        vertx = Vertx.vertx();
        JsonObject config = RedisquesConfiguration.with()
                .processorAddress(PROCESSOR_ADDRESS)
                .micrometerMetricsEnabled(true)
                .micrometerPerQueueMetricsEnabled(true)
                .micrometerMetricsIdentifier(metricsIdentifier)
                .refreshPeriod(2)
                .publishMetricsAddress("my-metrics-eb-address")
                .metricStorageName("foobar")
                .metricRefreshPeriod(2)
                .memoryUsageLimitPercent(80)
                .redisReadyCheckIntervalMs(2000)
                .queueConfigurations(List.of(new QueueConfiguration()
                                .withPattern("queue.*")
                                .withRetryIntervals(2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52),
                        new QueueConfiguration()
                                .withPattern("limited-queue-1.*")
                                .withMaxQueueEntries(1),
                        new QueueConfiguration()
                                .withPattern("limited-queue-4.*")
                                .withMaxQueueEntries(4))
                )
                .build()
                .asJsonObject();

        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        exceptionFactory = newWastefulExceptionFactory();
        memoryUsageProvider = new TestMemoryUsageProvider(Optional.of(50));
        redisQues = RedisQues.builder()
                .withMemoryUsageProvider(memoryUsageProvider)
                .withRedisquesRedisquesConfigurationProvider(new DefaultRedisquesConfigurationProvider(vertx, config))
                .withMeterRegistry(meterRegistry)
                .build();

        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - {} was successful.", redisQues.getClass().getSimpleName());
            jedis = new Jedis("localhost", 6379, 5000);
        }));

    }

    private void waitMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testUpdateTimestamp(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        final String fakeConsumerId = UUID.randomUUID().toString();
        final String queueNameForFakeConsumer = "queue-another-consumer-ts-test";

        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);
        long rangeStartTs = System.currentTimeMillis();
        queueRegistryService.updateTimestamp(queueNameForFakeConsumer).onComplete(event -> {
            if (event.failed()) {
                context.fail();
            } else {
                Set<String> queuesInRange = jedis.zrangeByScore(redisQues.getKeyspaceHelper().getQueuesKey(), rangeStartTs, rangeStartTs + 300);
                // The queue belong to dead consumer should update the time stamp.
                Assert.assertEquals(1, queuesInRange.size());
                Assert.assertEquals(queueNameForFakeConsumer, queuesInRange.iterator().next());
                waitMillis(500);
                queueRegistryService.updateTimestamp(queueNameForFakeConsumer).onComplete(event1 -> {
                    if (event1.failed()) {
                        context.fail();
                    } else {
                        Set<String> queuesInRange1 = jedis.zrangeByScore(redisQues.getKeyspaceHelper().getQueuesKey(), rangeStartTs, rangeStartTs + 300);
                        // The queue belong to dead consumer should update the time stamp.
                        Assert.assertEquals(0, queuesInRange1.size());
                        async.complete();
                    }
                });
            }
        });
    }

    /**
     * Test that a queue registered in the dead consumer should be recover once notifyConsumer called
     *
     * @param context
     */
    @Test
    public void testDeadQueueConsumerReRunByNotifyConsumer(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        final String fakeConsumerId = UUID.randomUUID().toString();
        final String queueNameForFakeConsumer = "queue-another-consumer-1-test";

        queueRegistryService.aliveConsumers.add(fakeConsumerId);
        Promise<Void> fakeConsumerPromise = Promise.promise();

        vertx.eventBus().consumer(fakeConsumerId).handler(event -> fakeConsumerPromise.complete());
        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);
        waitMillis(500);
        // this queue should not in real consumer
        Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, queueNameForFakeConsumer);
        eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-1"), e1 -> {
            eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-2"), e2 -> {
                eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-1"), e3 -> {
                    eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-2"), e4 -> {
                        queueRegistryService.notifyConsumer(queueNameForFakeConsumer).onComplete(event -> {
                            if (event.failed()) {
                                context.fail();
                            }
                            fakeConsumerPromise.future().onComplete(event2 -> {
                                if (event2.failed()) {
                                    context.fail();
                                }
                                waitMillis(500);
                                // this queue should not in real consumer
                                Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                // remove the fake consumer from alive list
                                queueRegistryService.aliveConsumers.remove(fakeConsumerId);

                                // notify once more
                                queueRegistryService.notifyConsumer(queueNameForFakeConsumer).onComplete(event1 -> {
                                    if (event1.failed()) {
                                        context.fail();
                                    }
                                    waitMillis(500);
                                    // this queue should not in real consumer
                                    Assert.assertTrue(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                    async.complete();
                                });
                            });
                        });

                    });
                });
            });
        });
    }

    /**
     * Test that a queue registered in the dead consumer should be recover once enqueue a new item
     *
     * @param context
     */
    @Test
    public void testDeadQueueConsumerReRunByEnqueueAction(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        final String fakeConsumerId = UUID.randomUUID().toString();
        final String queueNameForFakeConsumer = "queue-another-consumer-2-test";
        queueRegistryService.aliveConsumers.add(fakeConsumerId);

        Promise<Void> fakeConsumerPromise = Promise.promise();
        vertx.eventBus().consumer(fakeConsumerId).handler(event -> fakeConsumerPromise.complete());
        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);
        waitMillis(500);
        // this queue should not in real consumer
        Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, queueNameForFakeConsumer);
        eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-1"), e1 -> {
            eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-2"), e2 -> {
                eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-1"), e3 -> {
                    eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-2"), e4 -> {
                        queueRegistryService.notifyConsumer(queueNameForFakeConsumer).onComplete(event -> {
                            if (event.failed()) {
                                context.fail();
                            }
                            fakeConsumerPromise.future().onComplete(event2 -> {
                                if (event2.failed()) {
                                    context.fail();
                                }
                                waitMillis(500);
                                // this queue should not in real consumer
                                Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                // remove the fake consumer from alive list
                                queueRegistryService.aliveConsumers.remove(fakeConsumerId);
                                eventBusSend(buildEnqueueOperation(queueNameForFakeConsumer, "message_2-2"), event1 -> {
                                    if (event1.failed()) {
                                        context.fail(event1.cause());
                                    }
                                    waitMillis(500);
                                    // this queue should not in real consumer
                                    Assert.assertTrue(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                    async.complete();
                                });
                            });
                        });
                    });
                });
            });
        });
    }

    /**
     * Test that a queue registered in the dead consumer should be recover once CheckQueues triggered
     *
     * @param context
     */
    @Test
    public void testDeadQueueConsumerReRunByCheckQueues(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        final String fakeConsumerId = UUID.randomUUID().toString();
        final String queueNameForFakeConsumer = "queue-another-consumer-3-test";
        queueRegistryService.aliveConsumers.add(fakeConsumerId);

        Promise<Void> fakeConsumerPromise = Promise.promise();
        vertx.eventBus().consumer(fakeConsumerId).handler(event -> fakeConsumerPromise.complete());
        waitMillis(500);
        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);

        // this queue should not in real consumer
        Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, queueNameForFakeConsumer);
        eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-1"), e1 -> {
            eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-2"), e2 -> {
                eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-1"), e3 -> {
                    eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-2"), e4 -> {
                        queueRegistryService.notifyConsumer(queueNameForFakeConsumer).onComplete(event -> {
                            if (event.failed()) {
                                context.fail();
                            }
                            fakeConsumerPromise.future().onComplete(event2 -> {
                                if (event2.failed()) {
                                    context.fail();
                                }
                                waitMillis(500);
                                // this queue should not in real consumer
                                Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));

                                // remove the fake consumer from alive list
                                queueRegistryService.aliveConsumers.remove(fakeConsumerId);
                                queueRegistryService.checkQueues().onComplete(event1 -> {
                                    if (event1.failed()) {
                                        context.fail();
                                    }
                                    waitMillis(500);
                                    // this queue should not in real consumer
                                    Assert.assertTrue(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                    async.complete();
                                });
                            });
                        });
                    });
                });
            });
        });
    }

    @Test
    public void testMyQueuesSort(TestContext context) {
        Map<String, QueueProcessingState> myQueues = new HashMap<>();
        myQueues.put("queue-1", new QueueProcessingState(QueueState.READY, 500));
        myQueues.put("queue-2", new QueueProcessingState(QueueState.READY, 400));
        myQueues.put("queue-3", new QueueProcessingState(QueueState.READY, 300));
        myQueues.put("queue-4", new QueueProcessingState(QueueState.READY, 200));
        myQueues.put("queue-5", new QueueProcessingState(QueueState.READY, 100));
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        myQueues = queueRegistryService.getSortedMyQueueClone(myQueues);
        Iterator<Map.Entry<String, QueueProcessingState>> inherit = myQueues.entrySet().iterator();
        Map.Entry<String, QueueProcessingState> entry = inherit.next();
        Assert.assertEquals("queue-5", entry.getKey());
        Assert.assertEquals(100, entry.getValue().getLastRegisterRefreshedMillis());
        entry = inherit.next();
        Assert.assertEquals("queue-4", entry.getKey());
        Assert.assertEquals(200, entry.getValue().getLastRegisterRefreshedMillis());
        entry = inherit.next();
        Assert.assertEquals("queue-3", entry.getKey());
        Assert.assertEquals(300, entry.getValue().getLastRegisterRefreshedMillis());
        entry = inherit.next();
        Assert.assertEquals("queue-2", entry.getKey());
        Assert.assertEquals(400, entry.getValue().getLastRegisterRefreshedMillis());
        entry = inherit.next();
        Assert.assertEquals("queue-1", entry.getKey());
        Assert.assertEquals(500, entry.getValue().getLastRegisterRefreshedMillis());
    }

    @Test
    @Ignore // Can run at local only
    public void testAliveConsumerListUpdate(TestContext context) {
        final String fakeConsumerId = UUID.randomUUID().toString();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.aliveConsumers.add(fakeConsumerId);
        Assert.assertEquals(2, queueRegistryService.aliveConsumers.size());
        waitMillis(5000);
        Assert.assertEquals(1, queueRegistryService.aliveConsumers.size());
        jedis.set(redisQues.getKeyspaceHelper().getAliveConsumersPrefix() + "another-fake-id1", "another-fake-id1", new SetParams().px(2000));
        jedis.set(redisQues.getKeyspaceHelper().getAliveConsumersPrefix() + "another-fake-id2", "another-fake-id2", new SetParams().px(4000));
        waitMillis(800);
        Assert.assertEquals(1, queueRegistryService.aliveConsumers.size());
        waitMillis(1500);
        Assert.assertEquals(3, queueRegistryService.aliveConsumers.size());
        waitMillis(1000);
        Assert.assertEquals(2, queueRegistryService.aliveConsumers.size());
        waitMillis(2000);
        Assert.assertEquals(1, queueRegistryService.aliveConsumers.size());
    }
}
