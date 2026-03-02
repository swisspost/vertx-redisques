package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(VertxUnitRunner.class)
public class QueueConfigurationProviderTest {
    @Test
    public void testQueueConfigurationProviderCreation(TestContext context) {
        Async async = context.async();
        Vertx vertx = Vertx.vertx();
        RedisquesConfigurationProvider redisquesConfigurationProvider = Mockito.mock(RedisquesConfigurationProvider.class);
        QueueConfigurationProvider.provider(vertx, redisquesConfigurationProvider).get().onComplete(event -> {
            QueueConfigurationProvider provider1 = event.result();
            vertx.executeBlocking(promise -> {
                QueueConfigurationProvider.provider(vertx, redisquesConfigurationProvider).get().onComplete(event1 -> {
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
        RedisquesConfigurationProvider redisquesConfigurationProvider = Mockito.mock(RedisquesConfigurationProvider.class);
        QueueConfigurationProvider.provider(vertx, redisquesConfigurationProvider).get().onComplete(event -> {
            QueueConfigurationProvider provider = event.result();
            context.assertNull(provider.findQueueConfiguration("mytestqueue"));
            JsonObject jsonObject = new JsonObject();
            JsonArray jsonArray = new JsonArray();
            jsonArray.add(1);
            jsonArray.add(3);
            jsonArray.add(5);

            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_RETRY_INTERVALS, jsonArray);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS, 22);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS, 11);
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_ENTRIES, 99);
            provider.updateQueueConfiguration("mytestqueue", jsonObject);
            QueueConfiguration queueConfiguration = provider.findQueueConfiguration("mytestqueue");

            context.assertEquals(99,  queueConfiguration.getMaxQueueEntries());
            context.assertEquals(11.0F,  queueConfiguration.getEnqueueDelayFactorMillis());
            context.assertEquals(22,  queueConfiguration.getEnqueueMaxDelayMillis());
            context.assertEquals(3,  queueConfiguration.getRetryIntervals().length);

            JsonObject jsonObjectUpdate = new JsonObject();
            jsonObject.put(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS, 212);
            provider.updateQueueConfiguration("mytestqueue", jsonObject);
            QueueConfiguration queueConfigurationNew = provider.findQueueConfiguration("mytestqueue");

            // the onject should be updated, not replaced
            context.assertEquals(queueConfiguration,  queueConfigurationNew);
            context.assertEquals(99,  queueConfiguration.getMaxQueueEntries());
            context.assertEquals(11.0F,  queueConfiguration.getEnqueueDelayFactorMillis());
            context.assertEquals(212,  queueConfiguration.getEnqueueMaxDelayMillis());
            context.assertEquals(3,  queueConfiguration.getRetryIntervals().length);
            async.complete();
        });
    }
}