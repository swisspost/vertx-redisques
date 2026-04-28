package org.swisspush.redisques.queue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.redis.client.Response;
import org.junit.After;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
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
    public void testUnregisterConsumers(TestContext context) {
        Async async = context.async();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        List<MessageConsumer<?>> consumers = new ArrayList<>();
        consumers.add(vertx.eventBus().consumer("Test_Consumer_1", e -> {
            context.fail();
        }));
        consumers.add(vertx.eventBus().consumer("Test_Consumer_2", e -> {
            context.fail();
        }));

        queueRegistryService.unregisterAll(consumers).onComplete(event -> {
            vertx.eventBus().send("Test_Consumer_1", "");
            vertx.eventBus().send("Test_Consumer_2", "");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            async.complete();
        });
    }

    @Test
    public void testGracefulStop(TestContext context) {
        Async async = context.async();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.gracefulStop(new Handler<Void>() {
            @Override
            public void handle(Void event) {
                async.complete();
            }
        });
    }

    @Test
    public void testUpdateTimestampsWithOverwriteScore(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, "queue1-test-updatetimestamps");
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, "queue3-test-updatetimestamps");
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, "queue5-test-updatetimestamps");
        jedis.zadd(redisQues.getKeyspaceHelper().getQueuesKey(), 1, "queue7-test-updatetimestamps");
        List<String> queueNames = new ArrayList<>();
        queueNames.add("queue1-test-updatetimestamps");
        queueNames.add("queue2-test-updatetimestamps");
        queueNames.add("queue3-test-updatetimestamps");
        queueRegistryService.updateTimestampsWithOverwrite(queueNames).onComplete(event -> {
            if (event.failed()) {
                context.fail();
                return;
            }
            List<Response> result = event.result();
            context.assertEquals(queueNames.size(), result.size());
            context.assertEquals(1, result.get(0).toInteger());
            context.assertEquals(1, result.get(1).toInteger());
            context.assertEquals(1, result.get(2).toInteger());

            context.assertTrue(jedis.zscore(redisQues.getKeyspaceHelper().getQueuesKey(), "queue1-test-updatetimestamps") != 1);
            context.assertTrue(jedis.zscore(redisQues.getKeyspaceHelper().getQueuesKey(), "queue2-test-updatetimestamps") != 1);
            context.assertTrue(jedis.zscore(redisQues.getKeyspaceHelper().getQueuesKey(), "queue3-test-updatetimestamps") != 1);
            context.assertNull(jedis.zscore(redisQues.getKeyspaceHelper().getQueuesKey(), "queue4-test-updatetimestamps"));
            context.assertTrue(jedis.zscore(redisQues.getKeyspaceHelper().getQueuesKey(), "queue5-test-updatetimestamps") == 1);
            context.assertTrue(jedis.zscore(redisQues.getKeyspaceHelper().getQueuesKey(), "queue7-test-updatetimestamps") == 1);
            async.complete();
        });
    }

    @Test
    public void testBatchRefreshRegistration(TestContext context) {
        Async async = context.async();
        flushAll();

        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue2-test-batchregistry", "another-value");
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue5-test-batchregistry", "another-value");
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue9-test-batchregistry", "another-value");
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        List<String> queueNames = new ArrayList<>();
        queueNames.add("queue1-test-batchregistry");
        queueNames.add("queue2-test-batchregistry");
        queueNames.add("queue3-test-batchregistry");
        queueNames.add("queue4-test-batchregistry");
        queueNames.add("queue5-test-batchregistry");
        queueRegistryService.batchRefreshRegistration(queueNames).onComplete(event -> {
            if (event.failed()) {
                context.fail();
                return;
            }
            List<Response> result = event.result();
            context.assertEquals(queueNames.size(), result.size());
            context.assertEquals(0, result.get(0).toInteger());
            context.assertEquals(1, result.get(1).toInteger());
            context.assertEquals(0, result.get(2).toInteger());
            context.assertEquals(0, result.get(3).toInteger());
            context.assertEquals(1, result.get(4).toInteger());
            async.complete();
        });
    }

    @Test
    public void testBatchCheckQueuesSize(TestContext context) {
        Async async = context.async();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        Map<String, String> queueNames = new LinkedHashMap<>();
        queueNames.put(redisQues.getKeyspaceHelper().getQueuesPrefix() + "queue1-test-batchsize", "queue1-test-batchsize");
        queueNames.put(redisQues.getKeyspaceHelper().getQueuesPrefix() + "queue2-test-batchsize", "queue2-test-batchsize");
        queueNames.put(redisQues.getKeyspaceHelper().getQueuesPrefix() + "queue0-test-batchsize", "queue0-test-batchsize");
        queueNames.put(redisQues.getKeyspaceHelper().getQueuesPrefix() + "queue3-test-batchsize", "queue3-test-batchsize");
        queueNames.put(redisQues.getKeyspaceHelper().getQueuesPrefix() + "queue4-test-batchsize", "queue4-test-batchsize");
        queueNames.put(redisQues.getKeyspaceHelper().getQueuesPrefix() + "queue5-test-batchsize", "queue5-test-batchsize");
        eventBusSend(buildAddQueueItemOperation("queue1-test-batchsize", "message_1-1"), e1 -> {
            eventBusSend(buildAddQueueItemOperation("queue1-test-batchsize", "message_1-2"), e2 -> {
                eventBusSend(buildAddQueueItemOperation("queue1-test-batchsize", "message_1-3"), e3 -> {
                    eventBusSend(buildAddQueueItemOperation("queue2-test-batchsize", "message_2-1"), e4 -> {
                        eventBusSend(buildAddQueueItemOperation("queue2-test-batchsize", "message_2-2"), e5 -> {
                            eventBusSend(buildAddQueueItemOperation("queue3-test-batchsize", "message_1-2"), e6 -> {
                                queueRegistryService.batchCheckQueuesSize(new ArrayList<>(queueNames.entrySet())).onComplete(event -> {
                                    if (event.failed()) {
                                        context.fail();
                                        return;
                                    }
                                    Map<String, Integer> result = event.result();
                                    context.assertEquals(queueNames.size(), result.size());
                                    context.assertTrue(result.containsKey("queue1-test-batchsize"));
                                    context.assertTrue(result.containsKey("queue2-test-batchsize"));
                                    context.assertTrue(result.containsKey("queue0-test-batchsize"));
                                    context.assertTrue(result.containsKey("queue3-test-batchsize"));
                                    context.assertTrue(result.containsKey("queue4-test-batchsize"));
                                    context.assertTrue(result.containsKey("queue5-test-batchsize"));

                                    context.assertEquals(3, result.get("queue1-test-batchsize"));
                                    context.assertEquals(2, result.get("queue2-test-batchsize"));
                                    context.assertEquals(0, result.get("queue0-test-batchsize"));
                                    context.assertEquals(1, result.get("queue3-test-batchsize"));
                                    context.assertEquals(0, result.get("queue4-test-batchsize"));
                                    context.assertEquals(0, result.get("queue5-test-batchsize"));
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
    public void testbatchCheckIfImStillTheRegisteredConsumer(TestContext context) {
        Async async = context.async();
        flushAll();
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue-1", "consumer_id_1");
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue-2", "consumer_id_1");
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue-3", "consumer_id_1");
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue-4", "consumer_id_2");
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue-5", "consumer_id_3");
        jedis.set(redisQues.getKeyspaceHelper().getConsumersPrefix() + "queue-6", "");
        List<String> queuesToCheck = List.of("queue-1", "queue-2", "queue-3", "queue-5", "queue-6", "queue-7", "queue-8");

        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.batchCheckIfImStillTheRegisteredConsumer(queuesToCheck, "consumer_id_1").onComplete(event -> {
            if (event.failed()) {
                context.fail();
                return;
            }
            final List<String> myQueueNames = event.result().entrySet()
                    .stream()
                    .filter(e -> Boolean.TRUE.equals(e.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            final List<String> notMyQueueNames = event.result().entrySet()
                    .stream()
                    .filter(e -> Boolean.FALSE.equals(e.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            final List<String> notExistQueueNames = event.result().entrySet()
                    .stream()
                    .filter(e -> null == e.getValue())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            context.assertEquals(3, myQueueNames.size());
            context.assertTrue(myQueueNames.contains("queue-1"));
            context.assertTrue(myQueueNames.contains("queue-2"));
            context.assertTrue(myQueueNames.contains("queue-3"));

            context.assertEquals(2, notMyQueueNames.size());
            context.assertTrue(notMyQueueNames.contains("queue-5"));
            context.assertTrue(notMyQueueNames.contains("queue-6"));

            context.assertEquals(2, notExistQueueNames.size());
            context.assertTrue(notExistQueueNames.contains("queue-7"));
            context.assertTrue(notExistQueueNames.contains("queue-8"));
            async.complete();

        });
    }

    @Test
    public void testGetAllValidConsumerIds(TestContext context) throws InterruptedException {
        Async async = context.async();
        flushAll();
        final long keyLiveTime = 3000;
        Thread.sleep(2000);
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), System.currentTimeMillis() - 1000, "another-fake-id1");
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), System.currentTimeMillis() - 500, "another-fake-id2");
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), System.currentTimeMillis() - 1500, "another-fake-id3");
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), 0, "another-fake-id4");
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), 3500, "another-fake-id5");

        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.getAllValidConsumerIds(keyLiveTime).onComplete(event -> {
            if (event.failed()) {
                context.fail();
                return;
            }
            List<String> result = event.result();
            context.assertEquals(4, result.size());
            context.assertTrue(result.contains("another-fake-id1"));
            context.assertTrue(result.contains("another-fake-id2"));
            context.assertTrue(result.contains("another-fake-id3"));
            // my self also need shows up after delays
            context.assertTrue(result.contains(redisQues.getUid()));
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            queueRegistryService.getAllValidConsumerIds(keyLiveTime).onComplete(event1 -> {
                if (event1.failed()) {
                    context.fail();
                    return;
                }
                List<String> result1 = event1.result();
                context.assertEquals(2, result1.size());
                context.assertTrue(result1.contains("another-fake-id2"));
                // my self also need shows up after delays
                context.assertTrue(result1.contains(redisQues.getUid()));
                async.complete();
            });
        });
    }

    @Test
    public void testUpdateConsumerIdAndRemoveExpired(TestContext context) throws InterruptedException {
        Async async = context.async();
        final long keyLiveTime = 2000;
        flushAll();
        Thread.sleep(2000);
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), System.currentTimeMillis() - 5000, "another-fake-id2");
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.updateConsumerIdAndRemoveExpired("another-fake-id1", keyLiveTime).onComplete(event -> {
            if (event.failed()) {
                context.fail();
                return;
            }
            queueRegistryService.getAllValidConsumerIds(keyLiveTime).onComplete(event1 -> {
                if (event1.failed()) {
                    context.fail();
                    return;
                }
                List<String> result1 = event1.result();
                context.assertEquals(2, result1.size());
                context.assertTrue(result1.contains("another-fake-id1"));
                // my self also need shows up after delays
                context.assertTrue(result1.contains(redisQues.getUid()));
                async.complete();
            });
        });
    }

    @Ignore // Can run at local only
    @Test
    public void testAliveConsumerListUpdate(TestContext context) {
        final String fakeConsumerId = UUID.randomUUID().toString();
        flushAll();
        QueueRegistryService queueRegistryService = redisQues.getQueueRegistryService();
        queueRegistryService.aliveConsumers.add(fakeConsumerId);
        Assert.assertEquals(2, queueRegistryService.aliveConsumers.size());
        waitMillis(5000);
        Assert.assertEquals(1, queueRegistryService.aliveConsumers.size());
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), System.currentTimeMillis() - 2000, "another-fake-id1");
        jedis.zadd(redisQues.getKeyspaceHelper().getAliveConsumersKey(), System.currentTimeMillis() - 1000, "another-fake-id2");
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
