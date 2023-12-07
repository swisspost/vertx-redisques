package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.util.HandlerUtil;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

/**
 * Retrieves in it's AsyncResult handler the speed summary of the queues matching the given filter
 * and returns the same in the event completion.
 */
public class GetQueuesSpeedHandler implements Handler<AsyncResult<Response>> {

    private static final Logger log = LoggerFactory.getLogger(GetQueuesSpeedHandler.class);
    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final QueueStatisticsCollector queueStatisticsCollector;

    public GetQueuesSpeedHandler(
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
            // apply the given filter in order to have only the queues for which we ar interested
            List<String> queues = HandlerUtil
                .filterByPattern(handleQueues.result(), filterPattern);
            queueStatisticsCollector.getQueuesSpeed(event, queues);
        } else {
            log.warn("Concealed error", new Exception(handleQueues.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
