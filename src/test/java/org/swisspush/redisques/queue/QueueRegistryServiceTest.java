package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.action.AbstractQueueAction;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.DefaultRedisquesConfigurationProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.TestMemoryUsageProvider;
import redis.clients.jedis.Jedis;

import java.util.List;
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

    private void wait500Ms() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testUpdateTimestampIgnoreConsumerCheck(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        final String fakeConsumerId = UUID.randomUUID().toString();
        final String queueNameForFakeConsumer = "queue-another-consumer-ts-test";

        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);
        long rangeStartTs = System.currentTimeMillis();
        queueRegistryService.updateTimestamp(queueNameForFakeConsumer, false, event -> {
            if (event.failed()) {
                context.fail();
            } else {
                Set<String> queuesInRange = jedis.zrangeByScore(redisQues.getKeyspaceHelper().getQueuesKey(), rangeStartTs, rangeStartTs + 300);
                // The queue belong to dead consumer should update the time stamp.
                Assert.assertEquals(1, queuesInRange.size());
                Assert.assertEquals(queueNameForFakeConsumer, queuesInRange.iterator().next());
                wait500Ms();
                queueRegistryService.updateTimestamp(queueNameForFakeConsumer, false, event1 -> {
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

    @Test
    public void testUpdateTimestampDoConsumerCheck(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        final String fakeConsumerId = UUID.randomUUID().toString();
        final String queueNameForFakeConsumer = "queue-another-consumer-ts-1-test";

        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);
        long rangeStartTs = System.currentTimeMillis();
        // force update time stamp
        queueRegistryService.updateTimestamp(queueNameForFakeConsumer, false, event -> {
            if (event.failed()) {
                context.fail();
            } else {
                Set<String> queuesInRange = jedis.zrangeByScore(redisQues.getKeyspaceHelper().getQueuesKey(), rangeStartTs, rangeStartTs + 300);
                // The queue belong to dead consumer should update the time stamp.
                Assert.assertEquals(1, queuesInRange.size());
                Assert.assertEquals(queueNameForFakeConsumer, queuesInRange.iterator().next());
                wait500Ms();
                queueRegistryService.updateTimestamp(queueNameForFakeConsumer, true, event1 -> {
                    if (event1.failed()) {
                        context.fail();
                    } else {
                        Set<String> queuesInRange1 = jedis.zrangeByScore(redisQues.getKeyspaceHelper().getQueuesKey(), rangeStartTs, rangeStartTs + 300);
                        // The queue belong to another consumer should not update the time stamp.
                        Assert.assertEquals(1, queuesInRange1.size());
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

        // a fake consumer never expired
        queueRegistryService.aliveConsumers.put(fakeConsumerId, Long.MAX_VALUE);
        Promise<Void> fakeConsumerPromise = Promise.promise();

        vertx.eventBus().consumer(fakeConsumerId).handler(event -> fakeConsumerPromise.complete());
        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);
        wait500Ms();
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
                                wait500Ms();
                                // this queue should not in real consumer
                                Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                // remove the fake consumer from alive list
                                queueRegistryService.aliveConsumers.remove(fakeConsumerId);

                                // notify once more
                                queueRegistryService.notifyConsumer(queueNameForFakeConsumer).onComplete(event1 -> {
                                    if (event1.failed()) {
                                        context.fail();
                                    }
                                    wait500Ms();
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
        queueRegistryService.aliveConsumers.put(fakeConsumerId, Long.MAX_VALUE); // a fake consumer never expired

        Promise<Void> fakeConsumerPromise = Promise.promise();
        vertx.eventBus().consumer(fakeConsumerId).handler(event -> fakeConsumerPromise.complete());
        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);
        wait500Ms();
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
                                wait500Ms();
                                // this queue should not in real consumer
                                Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                // remove the fake consumer from alive list
                                queueRegistryService.aliveConsumers.remove(fakeConsumerId);
                                eventBusSend(buildEnqueueOperation(queueNameForFakeConsumer, "message_2-2"), event1 -> {
                                    if (event1.failed()) {
                                        context.fail(event1.cause());
                                    }
                                    wait500Ms();
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
        queueRegistryService.aliveConsumers.put(fakeConsumerId, Long.MAX_VALUE); // a fake consumer never expired

        Promise<Void> fakeConsumerPromise = Promise.promise();
        vertx.eventBus().consumer(fakeConsumerId).handler(event -> fakeConsumerPromise.complete());
        wait500Ms();
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
                                wait500Ms();
                                // this queue should not in real consumer
                                Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));

                                // remove the fake consumer from alive list
                                queueRegistryService.aliveConsumers.remove(fakeConsumerId);
                                queueRegistryService.checkQueues().onComplete(event1 -> {
                                    if (event1.failed()) {
                                        context.fail();
                                    }
                                    wait500Ms();
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
     * enqueue should not update timestamp which register to another consumer, that will block CheckQueues
     *
     * @param context
     */
    @Test
    public void testDeadQueueConsumerQueueTimeStampShouldNotUpdateByEnqueueUntilIOwnIt(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        final String fakeConsumerId = UUID.randomUUID().toString();
        final String queueNameForFakeConsumer = "queue-another-consumer-4-test";
        queueRegistryService.aliveConsumers.put(fakeConsumerId, Long.MAX_VALUE); // a fake consumer never expired

        Promise<Void> fakeConsumerPromise = Promise.promise();
        vertx.eventBus().consumer(fakeConsumerId).handler(event -> fakeConsumerPromise.complete());
        wait500Ms();
        // register the queue item into fake consumer
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + queueNameForFakeConsumer, fakeConsumerId);

        // this queue should not in real consumer
        Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, queueNameForFakeConsumer);
        eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-1"), e1 -> {
            eventBusSend(buildAddQueueItemOperation("queue1-test", "message_1-2"), e2 -> {
                eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-1"), e3 -> {
                    eventBusSend(buildAddQueueItemOperation(queueNameForFakeConsumer, "message_2-2"), e4 -> {
                        //buildEnqueueOperation will trigger a notifyConsumer, but timestamp will not update
                        eventBusSend(buildEnqueueOperation(queueNameForFakeConsumer, "message_2-3"), e5 -> {
                            if (e5.failed()) {
                                context.fail();
                            }

                            fakeConsumerPromise.future().onComplete(event2 -> {
                                if (event2.failed()) {
                                    context.fail();
                                }
                                wait500Ms();
                                // this queue should not in real consumer
                                Assert.assertFalse(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));

                                Set<String> queuesInRange = jedis.zrange(redisQues.getKeyspaceHelper().getQueuesKey(), 0, 1);
                                // The queue belong to dead consumer should not update the time stamp.
                                Assert.assertEquals(1, queuesInRange.size());
                                Assert.assertEquals(queueNameForFakeConsumer, queuesInRange.iterator().next());

                                // remove the fake consumer from alive list
                                queueRegistryService.aliveConsumers.remove(fakeConsumerId);
                                queueRegistryService.checkQueues().onComplete(x -> {
                                    wait500Ms();
                                    // this queue should in real consumer now
                                    Assert.assertTrue(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                    long rangeStartTs = System.currentTimeMillis();
                                    eventBusSend(buildEnqueueOperation(queueNameForFakeConsumer, "message_2-4"), event1 -> {
                                        if (event1.failed()) {
                                            context.fail();
                                        }
                                        wait500Ms();
                                        // Now it should belong to me and updated
                                        Set<String> queuesInRange1 = jedis.zrangeByScore(redisQues.getKeyspaceHelper().getQueuesKey(), rangeStartTs, rangeStartTs + 300);
                                        Assert.assertEquals(0, queuesInRange1.size());
                                        // this queue should still in real consumer now
                                        Assert.assertTrue(queueRegistryService.getQueueConsumerRunner().getMyQueues().containsKey(queueNameForFakeConsumer));
                                        async.complete();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }
}
