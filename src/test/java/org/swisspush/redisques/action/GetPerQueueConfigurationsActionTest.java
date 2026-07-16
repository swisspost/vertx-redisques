package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueConfigurationProvider;

import java.util.ArrayList;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GetPerQueueConfigurationsAction} class.
 */
@RunWith(VertxUnitRunner.class)
public class GetPerQueueConfigurationsActionTest {

    @Before
    public void setUp() {
        QueueConfigurationProvider.reset();
    }

    @Test
    public void testQueueConfigurationAction_getAll(TestContext context) {
        final Async async = context.async();
        Message<JsonObject> message = Mockito.mock(Message.class);
        ArgumentCaptor<JsonObject> captor = ArgumentCaptor.forClass(JsonObject.class);
        when(message.body()).thenReturn(new JsonObject("{\"operation\":\"getPerQueueConfiguration\",\"payload\":{\"filter\":\"*\"}}"));
        QueueConfigurationProvider.provider(Vertx.vertx(), new ArrayList<>(), 1_000).get().onComplete(event -> {
            QueueConfigurationProvider queueConfigurationProvider = event.result();
            GetPerQueueConfigurationsAction action = new GetPerQueueConfigurationsAction(queueConfigurationProvider, Mockito.mock(Logger.class));

            queueConfigurationProvider.updateQueueConfiguration("test-pattern-1", createQueueConfiguration("test-pattern-1").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-2", createQueueConfiguration("test-pattern-2").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-3", createQueueConfiguration("test-pattern-3").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-4", createQueueConfiguration("test-pattern-4").asJsonObject());
            action.execute(message);
            verify(message).reply(captor.capture());
            JsonObject value = captor.getValue();
            context.assertEquals(4, value.getMap().size());
            context.assertEquals("test-pattern-1", ((Map<?, ?>) captor.getValue().getMap().get("test-pattern-1")).get("pattern"));
            context.assertEquals("test-pattern-2", ((Map<?, ?>) captor.getValue().getMap().get("test-pattern-2")).get("pattern"));
            context.assertEquals("test-pattern-3", ((Map<?, ?>) captor.getValue().getMap().get("test-pattern-3")).get("pattern"));
            context.assertEquals("test-pattern-4", ((Map<?, ?>) captor.getValue().getMap().get("test-pattern-4")).get("pattern"));
            async.complete();
        });
    }

    @Test
    public void testQueueConfigurationAction_getSpecified(TestContext context) {
        final Async async = context.async();
        Message<JsonObject> message = Mockito.mock(Message.class);
        ArgumentCaptor<JsonObject> captor = ArgumentCaptor.forClass(JsonObject.class);
        when(message.body()).thenReturn(new JsonObject("{\"operation\":\"getPerQueueConfiguration\",\"payload\":{\"configName\":\"test-pattern-3\"}}"));
        QueueConfigurationProvider.provider(Vertx.vertx(), new ArrayList<>(), 1_000).get().onComplete(event -> {
            QueueConfigurationProvider queueConfigurationProvider = event.result();
            GetPerQueueConfigurationsAction action = new GetPerQueueConfigurationsAction(queueConfigurationProvider, Mockito.mock(Logger.class));

            queueConfigurationProvider.updateQueueConfiguration("test-pattern-1", createQueueConfiguration("test-pattern-1").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-2", createQueueConfiguration("test-pattern-2").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-3", createQueueConfiguration("test-pattern-3").asJsonObject());
            queueConfigurationProvider.updateQueueConfiguration("test-pattern-4", createQueueConfiguration("test-pattern-4").asJsonObject());
            action.execute(message);
            verify(message).reply(captor.capture());
            JsonObject value = captor.getValue();
            context.assertEquals(1, value.getMap().size());
            context.assertEquals("test-pattern-3", ((Map<?, ?>) captor.getValue().getMap().get("test-pattern-3")).get("pattern"));
            async.complete();
        });
    }

    private QueueConfiguration createQueueConfiguration(String pattern) {
        QueueConfiguration queueConfiguration = new QueueConfiguration(pattern);
        queueConfiguration.withEnqueueDelayMillisPerSize(99).withMaxQueueEntries(12).withEnqueueMaxDelayMillis(33).withRetryIntervals(1, 2, 3);
        return queueConfiguration;
    }
}
