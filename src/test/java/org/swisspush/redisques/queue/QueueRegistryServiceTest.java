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


import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newWastefulExceptionFactory;
import static org.swisspush.redisques.queue.QueueRegistryService.LOAD_BALANCE_SCORE_NOT_VALID;
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

        queueRegistryService.aliveConsumers.put(fakeConsumerId, LOAD_BALANCE_SCORE_NOT_VALID);
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
        queueRegistryService.aliveConsumers.put(fakeConsumerId, LOAD_BALANCE_SCORE_NOT_VALID);

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
        queueRegistryService.aliveConsumers.put(fakeConsumerId, LOAD_BALANCE_SCORE_NOT_VALID);

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
    public void testGetLoadScore_allQueuesAreEmpty(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        Set<String> queueSet = new HashSet<>();
        queueSet.add("queue-1");
        queueSet.add("queue-2");
        queueSet.add("queue-3");

        queueRegistryService.getMyLoadScore(queueSet, 1, 3)
                .onFailure(context::fail)
                .onSuccess(event -> {
                    // 3(3 queues) x 1 + 0(zero items) x 3
                    Assert.assertEquals(3, event.intValue());
                    async.complete();
                });
    }

    @Test
    public void testGetLoadScore_queueHaveItems(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        Set<String> queueSet = new HashSet<>();
        queueSet.add("queue-1");
        queueSet.add("queue-2");
        queueSet.add("queue-3");

        eventBusSend(buildAddQueueItemOperation("queue-1", "message_1-1"), e1 -> {
            eventBusSend(buildAddQueueItemOperation("queue-1", "message_1-2"), e2 -> {
                eventBusSend(buildAddQueueItemOperation("queue-1", "message_1-3"), e3 -> {
                    eventBusSend(buildAddQueueItemOperation("queue-3", "message_1-1"), e4 -> {

                        queueRegistryService.getMyLoadScore(queueSet, 1, 3)
                                .onFailure(context::fail)
                                .onSuccess(event -> {
                                    // 3(3 queues) x 1 + 4(4 items) x 3
                                    Assert.assertEquals(15, event.intValue());
                                    async.complete();
                                });
                    });
                });
            });
        });
    }

    @Test
    public void testFindRebalanceTarget_allConditions(TestContext context) {
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        Map<String, Long> scoreMap = new HashMap<>();

        // no score in list
        context.assertNull(queueRegistryService.findRebalanceTarget(scoreMap, "instance-0", 50, 10));

        scoreMap.put("instance-0", 150L);
        scoreMap.put("instance-1", 100L);
        scoreMap.put("instance-2", LOAD_BALANCE_SCORE_NOT_VALID);
        scoreMap.put("instance-3", 70L);

        // not all instance reported score
        context.assertNull(queueRegistryService.findRebalanceTarget(scoreMap, "instance-0", 50, 10));

        scoreMap.put("instance-2", 90L);
        scoreMap.put("instance-4", 30L);
        scoreMap.put("instance-5", 20L);

        // self have invalid score

        context.assertNull(queueRegistryService.findRebalanceTarget(scoreMap, "instance-0", LOAD_BALANCE_SCORE_NOT_VALID, 10));

        // I have the lowest score in the list: do noting
        context.assertNull(queueRegistryService.findRebalanceTarget(scoreMap, "instance-5", 22, 10));

        // my score is high, but not top one: send load to match one
        context.assertEquals("instance-4", queueRegistryService.findRebalanceTarget(scoreMap, "instance-2", 90, 10));

        // my score is top one
        context.assertEquals("instance-5", queueRegistryService.findRebalanceTarget(scoreMap, "instance-0", 140, 10));

        // my score is top one, with big margin
        context.assertEquals("instance-5", queueRegistryService.findRebalanceTarget(scoreMap, "instance-0", 140, 30));

        // my score is top one, with very big margin
        context.assertNull(queueRegistryService.findRebalanceTarget(scoreMap, "instance-0", 140, 80));
    }

    @Test
    public void testMoveLoadToOtherInstanceIfNeeded_noScoreToCalculate(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.moveLoadToOtherInstanceIfNeeded().onComplete(event -> {
            if (event.failed()) {
                context.fail(event.cause());
            } else {
                async.complete();
            }
        });
    }

    @Test
    @Ignore // Can run at local only
    public void testAliveConsumerListUpdate(TestContext context) {
        final String fakeConsumerId = UUID.randomUUID().toString();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.aliveConsumers.put(fakeConsumerId, LOAD_BALANCE_SCORE_NOT_VALID);
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
