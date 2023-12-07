package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.swisspush.redisques.util.HandlerUtil;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

/**
 * Retrieves in it's AsyncResult handler for all given queue names the queue statistics information
 * and returns the same in the event completion.
 */
public class GetQueuesStatisticsHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = getLogger(GetQueuesStatisticsHandler.class);
    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final QueueStatisticsCollector queueStatisticsCollector;

    public GetQueuesStatisticsHandler(
            Message<JsonObject> event,
            Optional<Pattern> filterPattern,
            QueueStatisticsCollector queueStatisticsCollector
    ) {
        this.event = event;
        this.filterPattern = filterPattern;
        this.queueStatisticsCollector = queueStatisticsCollector;
    }

    @Override
    public void handle(AsyncResult<Response> handleQueues) {
        if (handleQueues.succeeded()) {
            List<String> queues = HandlerUtil
                .filterByPattern(handleQueues.result(), filterPattern);
            queueStatisticsCollector.getQueueStatistics(event, queues);
        } else {
            log.warn("Concealed error", new Exception(handleQueues.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }

}
