package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.DeleteLockHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.QueueRegistryService;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

public class DeleteLockAction extends AbstractQueueAction {

    private final QueueRegistryService queueRegistryService;

    public DeleteLockAction(
            Vertx vertx, QueueRegistryService queueRegistryService, RedisService redisService, KeyspaceHelper keyspaceHelper, List<QueueConfiguration> queueConfigurations,
            RedisQuesExceptionFactory exceptionFactory, QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurations, exceptionFactory, queueStatisticsCollector, log);
        this.queueRegistryService = queueRegistryService;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        redisService.exists(Collections.singletonList(keyspaceHelper.getQueuesPrefix() + queueName)).onComplete(event1 -> {
            if (event1.failed()) {
                log.warn("Concealed error", exceptionFactory.newException(event1.cause()));
            }
            // delete lock first, to make sure the consumer can start process the queue right way.
            redisService.hdel(Arrays.asList(keyspaceHelper.getLocksKey(), queueName)).onComplete(response -> {
                new DeleteLockHandler(event, exceptionFactory).handle(response);
                // notify consumer
                if (event1.succeeded() && event1.result() != null && event1.result().toInteger() == 1) {
                    queueRegistryService.notifyConsumer(queueName);
                }
            });
        });
    }
}
