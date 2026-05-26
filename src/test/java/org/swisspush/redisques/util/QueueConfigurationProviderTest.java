package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class QueueConfigurationProviderTest {

    @Before
    public void setUp() {
        QueueConfigurationProvider.reset();
    }

    @Test
    public void testQueueConfigurationProviderCreation(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        QueueConfigurationProvider.provider(vertx, List.of()).get().onComplete(event -> {
            QueueConfigurationProvider provider1 = event.result();
            vertx.executeBlocking(promise -> {
                QueueConfigurationProvider.provider(vertx, List.of()).get().onComplete(event1 -> {
                    context.assertEquals(provider1, event1.result());
                    context.assertEquals(provider1.getUid(), event1.result().getUid());
                    async.complete();
                });
            });
        });
    }

    @Test
    public void testQueueConfigUpdate(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        QueueConfigurationProvider.provider(vertx, List.of()).get().onComplete(event -> {
            QueueConfigurationProvider provider = event.result();
            context.assertNull(provider.findQueueConfiguration("mytestqueue"));
            JsonObject jsonObject = new JsonObject();
            JsonArray jsonArray = new JsonArray();
            jsonArray.add(1);
            jsonArray.add(3);
            jsonArray.add(5);

            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_PATTERN, ".*mytestqueue*");
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_RETRY_INTERVALS, jsonArray);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS, 22);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS, 11);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_ENTRIES, 99);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MAXIMUM_ITEM_IN_BATCH_DISPATCH, 500);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MINIMUM_ITEM_IN_BATCH_DISPATCH, 400);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MAX_BATCH_DISPATCH_WAIT_TIMEOUT, 10);

            provider.updateQueueConfiguration("mytestqueue", jsonObject);
            QueueConfiguration queueConfiguration = provider.findQueueConfiguration("mytestqueue");

            context.assertEquals(99,  queueConfiguration.getMaxQueueEntries());
            context.assertEquals(11.0F,  queueConfiguration.getEnqueueDelayFactorMillis());
            context.assertEquals(22,  queueConfiguration.getEnqueueMaxDelayMillis());
            context.assertEquals(500,  queueConfiguration.getMaximumItemInBatchDispatch());
            context.assertEquals(400,  queueConfiguration.getMinimumItemInBatchDispatch());
            context.assertEquals(10,  queueConfiguration.getMaxBatchItemDispatchWaitTimeout());
            context.assertEquals(3,  queueConfiguration.getRetryIntervals().length);

            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS, 212);
            provider.updateQueueConfiguration("mytestqueue", jsonObject);
            QueueConfiguration queueConfigurationNew = provider.findQueueConfiguration("mytestqueue");

            // the object should be updated, not replaced
            context.assertEquals(queueConfiguration,  queueConfigurationNew);
            context.assertEquals(99,  queueConfiguration.getMaxQueueEntries());
            context.assertEquals(11.0F,  queueConfiguration.getEnqueueDelayFactorMillis());
            context.assertEquals(212,  queueConfiguration.getEnqueueMaxDelayMillis());
            context.assertEquals(3,  queueConfiguration.getRetryIntervals().length);
            async.complete();
        });
    }

    @Test
    public void testQueueConfigGetAll(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        QueueConfigurationProvider.provider(vertx, List.of()).get().onComplete(event -> {
            QueueConfigurationProvider provider = event.result();

            provider.updateQueueConfiguration("my_config_1", createQueueConfiguration("1.*").asJsonObject());
            provider.updateQueueConfiguration("my_config_2", createQueueConfiguration("2.*").asJsonObject());
            provider.updateQueueConfiguration("my_config_3", createQueueConfiguration("3.*").asJsonObject());
            Map<String, QueueConfiguration> queueConfigurations = provider.getQueueConfigurations("*");
            context.assertEquals(3,  queueConfigurations.size());
            async.complete();
        });
    }

    @Test
    public void testQueueConfigGetOne(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        QueueConfigurationProvider.provider(vertx, List.of()).get().onComplete(event -> {
            QueueConfigurationProvider provider = event.result();

            provider.updateQueueConfiguration("my_config_1", createQueueConfiguration("1.*").asJsonObject());
            provider.updateQueueConfiguration("my_config_2", createQueueConfiguration("2.*").asJsonObject());
            provider.updateQueueConfiguration("my_config_3", createQueueConfiguration("3.*").asJsonObject());
            Map<String, QueueConfiguration> queueConfigurations = provider.getQueueConfigurations("my_config_2");
            context.assertEquals(1,  queueConfigurations.size());
            async.complete();
        });
    }

    @Test
    public void testQueueConfigRemove(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        QueueConfigurationProvider.provider(vertx, List.of()).get().onComplete(event -> {
            QueueConfigurationProvider provider = event.result();

            provider.updateQueueConfiguration("my_config_1", createQueueConfiguration("1.*").asJsonObject());
            provider.updateQueueConfiguration("my_config_2", createQueueConfiguration("2.*").asJsonObject());
            provider.updateQueueConfiguration("my_config_3", createQueueConfiguration("3.*").asJsonObject());
            Map<String, QueueConfiguration> queueConfigurations = provider.getQueueConfigurations("*");
            context.assertEquals(3,  queueConfigurations.size());
            provider.removeQueueConfiguration("anything?");
            context.assertEquals(3,  queueConfigurations.size());
            provider.removeQueueConfiguration("my_config_1");
            context.assertEquals(2,  queueConfigurations.size());
            provider.removeQueueConfiguration("my_config_2");
            provider.removeQueueConfiguration("my_config_3");
            context.assertEquals(0,  queueConfigurations.size());
            provider.removeQueueConfiguration("my_config_3");
            context.assertEquals(0,  queueConfigurations.size());
            async.complete();
        });
    }

    @Test
    public void testQueueConfigCategoryRead(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        QueueConfigurationProvider.provider(vertx, List.of()).get().onComplete(event -> {
            QueueConfigurationProvider provider = event.result();
            context.assertNull(provider.findQueueConfiguration("mytestqueue"));
            JsonObject jsonObject = new JsonObject();
            JsonArray jsonArray = new JsonArray();
            jsonArray.add(1);
            jsonArray.add(3);
            jsonArray.add(5);

            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_PATTERN, ".*mytestqueue*");
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_RETRY_INTERVALS, jsonArray);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS, 22);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS, 11);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_ENTRIES, 99);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_PATROL_LIMIT, 42);
            provider.updateQueueConfiguration("mytestqueue", jsonObject);
            QueueConfiguration queueConfiguration = provider.findQueueConfiguration("mytestqueue");

            context.assertEquals(99, queueConfiguration.getMaxQueueEntries());
            context.assertEquals(11.0F, queueConfiguration.getEnqueueDelayFactorMillis());
            context.assertEquals(22, queueConfiguration.getEnqueueMaxDelayMillis());
            context.assertEquals(3, queueConfiguration.getRetryIntervals().length);
            context.assertEquals(42L, queueConfiguration.getEnqueuePatrolLimit());

            context.assertEquals(99, provider.findMaxQueueEntriesConfig("mytestqueue"));
            context.assertEquals(22L, provider.findEnqueueDelayConfig("mytestqueue", 4));
            context.assertEquals(42L, provider.findEnqueuePatrolConfig("mytestqueue"));
            context.assertEquals(3, provider.findRetryIntervalConfig("mytestqueue").size());


            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS, 0);
            provider.updateQueueConfiguration("mytestqueue", jsonObject);

            context.assertEquals(99, provider.findMaxQueueEntriesConfig("mytestqueue"));
            context.assertEquals(0L, provider.findEnqueueDelayConfig("mytestqueue", 4));
            context.assertEquals(42L, provider.findEnqueuePatrolConfig("mytestqueue"));
            context.assertEquals(3, provider.findRetryIntervalConfig("mytestqueue").size());

            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_PATROL_LIMIT, 0);
            provider.updateQueueConfiguration("mytestqueue", jsonObject);
            context.assertEquals(99, provider.findMaxQueueEntriesConfig("mytestqueue"));
            context.assertEquals(0L, provider.findEnqueueDelayConfig("mytestqueue", 4));
            context.assertEquals(0L, provider.findEnqueuePatrolConfig("mytestqueue"));
            context.assertEquals(3, provider.findRetryIntervalConfig("mytestqueue").size());

            async.complete();
        });
    }

    private QueueConfiguration createQueueConfiguration(String pattern) {
        QueueConfiguration queueConfiguration = new QueueConfiguration(pattern);
        queueConfiguration.withEnqueueDelayMillisPerSize(99).withMaxQueueEntries(12).withEnqueueMaxDelayMillis(33).withRetryIntervals(1, 2, 3);
        return queueConfiguration;
    }
}