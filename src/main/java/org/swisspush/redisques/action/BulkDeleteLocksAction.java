package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.MultiType;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;


import static org.swisspush.redisques.util.RedisquesAPI.*;

public class BulkDeleteLocksAction extends AbstractQueueAction {

    public BulkDeleteLocksAction(Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper, QueueConfigurationProvider queueConfigurationProvider,
                                 RedisquesConfigurationProvider redisquesConfigurationProvider,
                                 RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log) {
        super(vertx, redisService, keyspaceHelper, queueConfigurationProvider, redisquesConfigurationProvider,
                exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonArray jsonArray = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (jsonArray != null) {
            MultiType locks = MultiType.create(jsonArray.size(), false);
            for (int j = 0; j < jsonArray.size(); j++) {
                Response response = SimpleStringType.create(jsonArray.getString(j));
                locks.add(response);
            }
            deleteLocks(event, locks);
        } else {
            event.reply(createErrorReply().put(MESSAGE, "No locks to delete provided"));
        }
    }
}
