package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.MessageConsumerManager;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueConfigurationProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GetPerQueueConfigurationsAction} class.
 */
@RunWith(VertxUnitRunner.class)
public class GetPerQueueConfigurationsActionTest {



    private final List<Vertx> vertxInstances = new CopyOnWriteArrayList<>();

    private Vertx newVertx() {
        Vertx vertx = Vertx.vertx();
        vertxInstances.add(vertx);
        return vertx;
    }

    @Before
    public void setUp() {
        QueueConfigurationProvider.reset();
    }

    @After
    public void tearDown() throws Exception {
        for (Vertx vertx : vertxInstances) {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        vertxInstances.clear();
    }

    @Test
    public void testQueueConfigurationAction_getAll(TestContext context) {
        final Async async = context.async();
            final Vertx vertx = newVertx();
        Message<JsonObject> message = Mockito.mock(Message.class);
        ArgumentCaptor<JsonObject> captor = ArgumentCaptor.forClass(JsonObject.class);
        when(message.body()).thenReturn(new JsonObject("{\"operation\":\"getPerQueueConfiguration\",\"payload\":{\"filter\":\"*\"}}"));
        QueueConfigurationProvider.provider(vertx, new ArrayList<>(), 1_000).get().onComplete(event -> {
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
            final Vertx vertx = newVertx();
        Message<JsonObject> message = Mockito.mock(Message.class);
        ArgumentCaptor<JsonObject> captor = ArgumentCaptor.forClass(JsonObject.class);
        when(message.body()).thenReturn(new JsonObject("{\"operation\":\"getPerQueueConfiguration\",\"payload\":{\"configName\":\"test-pattern-3\"}}"));
        QueueConfigurationProvider.provider(vertx, new ArrayList<>(), 1_000).get().onComplete(event -> {
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
