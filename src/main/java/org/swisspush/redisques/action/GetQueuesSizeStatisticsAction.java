package org.swisspush.redisques.action;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

/**
 * Retrieve the queue statistics info of the requested queues
 */
public class GetQueuesSizeStatisticsAction extends AbstractQueueAction {

    public GetQueuesSizeStatisticsAction(
            Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper,
            QueueConfigurationProvider queueConfigurationProvider,
            RedisquesConfigurationProvider redisquesConfigurationProvider, RedisQuesExceptionFactory exceptionFactory,
            QueueStatisticsCollector queueStatisticsCollector, Logger log
    ) {
        super(vertx, redisService, keyspaceHelper,
                queueConfigurationProvider, redisquesConfigurationProvider, exceptionFactory, queueStatisticsCollector, log);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        getQueuesSizeStatistics(event);
    }

    /**
     * Retrieve the queue size statistics info
     */
    private void getQueuesSizeStatistics(Message<JsonObject> event) {
        queueStatisticsCollector.getAllApproximateQueueSize().onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                event.reply(createErrorReply().put(MESSAGE, asyncResult.cause().getMessage()));
            } else {
                event.reply(new JsonObject().put(STATUS, OK).put(PAYLOAD, asyncResult.result()));
            }
        });
    }
}
