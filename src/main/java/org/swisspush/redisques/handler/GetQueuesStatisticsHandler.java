package org.swisspush.redisques.handler;

import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.util.HandlerUtil;
import org.swisspush.redisques.util.QueueStatisticsCollector;

/**
 * Retrieves in it's AsyncResult handler for all given queue names the queue statistics information
 * and returns the same in the event completion.
 */
public class GetQueuesStatisticsHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = LoggerFactory.getLogger(GetQueuesStatisticsHandler.class);
    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final QueueStatisticsCollector queueStatisticsCollector;

    public GetQueuesStatisticsHandler(Message<JsonObject> event,
        Optional<Pattern> filterPattern,
        QueueStatisticsCollector queueStatisticsCollector) {
        this.event = event;
        this.filterPattern = filterPattern;
        this.queueStatisticsCollector = queueStatisticsCollector;
    }

    @Override
    public void handle(AsyncResult<Response> handleQueues) {
        if (handleQueues.succeeded()) {
            List<String> queues = HandlerUtil
                    .filterByPattern(handleQueues.result(), filterPattern);
            queueStatisticsCollector.getQueueStatistics(queues)
                    .onFailure(ex -> {
                        log.error("", ex);
                        event.reply(new JsonObject().put(STATUS, ERROR));
                    })
                    .onSuccess(event::reply);
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
