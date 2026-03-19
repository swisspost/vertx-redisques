package org.swisspush.redisques.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class QueueConfigurationTest {

    @Test
    public void testQueueConfiguration_toJsonObject(TestContext context) {
        QueueConfiguration queueConfiguration = new QueueConfiguration()
                .withPattern("queue-in-myserver.*")
                .withRetryIntervals(10)
                .withMaxQueueEntries(1)
                .withEnqueueDelayMillisPerSize(22)
                .withEnqueueMaxDelayMillis(33);

        JsonObject jsonObject = queueConfiguration.asJsonObject();
        context.assertNotNull(jsonObject);
        context.assertEquals("queue-in-myserver.*", jsonObject.getString("pattern"));
        context.assertEquals(1, jsonObject.getJsonArray("retryIntervals").size());
        context.assertEquals(10, jsonObject.getJsonArray("retryIntervals").getList().get(0));
        context.assertEquals(22L, jsonObject.getLong("enqueueDelayFactorMillis"));
        context.assertEquals(33, jsonObject.getInteger("enqueueMaxDelayMillis"));
        context.assertEquals(1, jsonObject.getInteger("maxQueueEntries"));
    }

    @Test
    public void testQueueConfiguration_FromJsonObject(TestContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("pattern", "queue-in-myserver.*");

        JsonArray jsonArray = new JsonArray();
        jsonArray.add(10);
        jsonArray.add(20);
        jsonObject.put("retryIntervals", jsonArray);

        jsonObject.put("maxQueueEntries", 11);
        jsonObject.put("enqueueDelayFactorMillis", 22);
        jsonObject.put("enqueueMaxDelayMillis", 33);


        QueueConfiguration queueConfiguration = QueueConfiguration.fromJsonObject(jsonObject);
        context.assertNotNull(queueConfiguration);
        context.assertEquals("queue-in-myserver.*", queueConfiguration.getPattern());
        context.assertEquals(2, queueConfiguration.getRetryIntervals().length);
        context.assertEquals(11, queueConfiguration.getMaxQueueEntries());
        context.assertEquals(22.0F, queueConfiguration.getEnqueueDelayFactorMillis());
        context.assertEquals(33, queueConfiguration.getEnqueueMaxDelayMillis());

    }
}
