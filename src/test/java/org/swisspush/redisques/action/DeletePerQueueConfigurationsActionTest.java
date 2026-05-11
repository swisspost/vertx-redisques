package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;

import java.util.ArrayList;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link DeletePerQueueConfigurationsAction} class.
 */
@RunWith(VertxUnitRunner.class)
public class DeletePerQueueConfigurationsActionTest {
    @Test
    public void testQueueConfigurationAction_deleteOneConfig(TestContext context) {
        final Async async = context.async();
        Message<JsonObject> message = Mockito.mock(Message.class);
        QueueConfigurationProvider.provider(Vertx.vertx(), new ArrayList<>()).get().onComplete(event -> {
            QueueConfigurationProvider queueConfigurationProvider = event.result();
            DeletePerQueueConfigurationsAction action = new DeletePerQueueConfigurationsAction(queueConfigurationProvider, Mockito.mock(Logger.class));
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-1", createQueueConfiguration("test-pattern-1").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-2", createQueueConfiguration("test-pattern-2").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-3", createQueueConfiguration("test-pattern-3").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-4", createQueueConfiguration("test-pattern-4").asJsonObject());

            when(message.body()).thenReturn(RedisquesAPI.buildDeletePerQueueConfiguration("test-pattern-3"));

            action.execute(message);
            Map<String, QueueConfiguration> configurationMap = queueConfigurationProvider.getQueueConfigurations("*");
            context.assertEquals(3, configurationMap.size());
            context.assertTrue(configurationMap.containsKey("test-pattern-1"));
            context.assertTrue(configurationMap.containsKey("test-pattern-2"));
            context.assertFalse(configurationMap.containsKey("test-pattern-3"));
            context.assertTrue(configurationMap.containsKey("test-pattern-4"));
            async.complete();
        });
    }

    @Test
    public void testQueueConfigurationAction_deleteNothing(TestContext context) {
        final Async async = context.async();
        Message<JsonObject> message = Mockito.mock(Message.class);
        QueueConfigurationProvider.provider(Vertx.vertx(), new ArrayList<>()).get().onComplete(event -> {
            QueueConfigurationProvider queueConfigurationProvider = event.result();
            DeletePerQueueConfigurationsAction action = new DeletePerQueueConfigurationsAction(queueConfigurationProvider, Mockito.mock(Logger.class));
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-1", createQueueConfiguration("test-pattern-1").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-2", createQueueConfiguration("test-pattern-2").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-3", createQueueConfiguration("test-pattern-3").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-4", createQueueConfiguration("test-pattern-4").asJsonObject());

            when(message.body()).thenReturn(RedisquesAPI.buildDeletePerQueueConfiguration("*"));

            action.execute(message);
            Map<String, QueueConfiguration> configurationMap = queueConfigurationProvider.getQueueConfigurations("*");
            context.assertEquals(4, configurationMap.size());
            context.assertTrue(configurationMap.containsKey("test-pattern-1"));
            context.assertTrue(configurationMap.containsKey("test-pattern-2"));
            context.assertTrue(configurationMap.containsKey("test-pattern-3"));
            context.assertTrue(configurationMap.containsKey("test-pattern-4"));
            async.complete();
        });
    }

    private QueueConfiguration createQueueConfiguration(String pattern) {
        QueueConfiguration queueConfiguration = new QueueConfiguration(pattern);
        queueConfiguration.withEnqueueDelayMillisPerSize(99).withMaxQueueEntries(12).withEnqueueMaxDelayMillis(33).withRetryIntervals(1, 2, 3);
        return queueConfiguration;
    }
}
