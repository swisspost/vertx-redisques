package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(VertxUnitRunner.class)
public class QueueConfigurationProviderTest {
    @Test
    public void testQueueConfigurationProvider(TestContext context) {
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
}