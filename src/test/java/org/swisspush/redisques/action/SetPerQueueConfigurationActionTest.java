package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;

/**
 * Tests for {@link SetPerQueueConfigurationAction} class.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
@RunWith(VertxUnitRunner.class)
public class SetPerQueueConfigurationActionTest {
    private QueueConfigurationProvider queueConfigurationProvider = Mockito.mock(QueueConfigurationProvider.class);
    private SetPerQueueConfigurationAction action = new SetPerQueueConfigurationAction(queueConfigurationProvider, Mockito.mock(Logger.class));

    @Test
    public void testQueueConfigurationAction() {
        Message<JsonObject> message = Mockito.mock(Message.class);
        JsonObject payload = new JsonObject();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(10);
        jsonArray.add(20);
        payload.put("retryIntervals", jsonArray);

        payload.put("maxQueueEntries", 11);
        payload.put("enqueueDelayFactorMillis", 22);
        payload.put("enqueueMaxDelayMillis", 33);
        JsonObject config = RedisquesAPI.buildSetPerQueueConfiguration("my-queue", payload);
        Mockito.when(message.body()).thenReturn(config);
        action.execute(message);

        Mockito.verify(queueConfigurationProvider).updateQueueConfiguration("my-queue", payload);
    }
}
