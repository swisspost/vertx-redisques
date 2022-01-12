package org.swisspush.redisques.util;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class QueueHandlerUtil {

    private static final Logger log = LoggerFactory.getLogger(QueueHandlerUtil.class);

    /**
     * Apply the given filter pattern to the given JsonArray containing the list of queues.
     * @param allQueues list of JSON objects containing the queueName to be filtered
     * @param filterPattern The filter regex pattern to be matched against the given queues list
     * @return The resulting filtered list of queues. If there is no filter given, the full list
     *         of queues is returned.
     */
    public static List<String> filterQueues(JsonArray allQueues, Optional<Pattern> filterPattern) {
        List<String> queues = new ArrayList<>();
        if (filterPattern != null && filterPattern.isPresent()) {
            Pattern pattern = filterPattern.get();
            for (int i = 0; i < allQueues.size(); i++) {
                String queue = allQueues.getString(i);
                if (pattern.matcher(queue).find()) {
                    queues.add(queue);
                }
            }
        } else {
            for (int i = 0; i < allQueues.size(); i++) {
                queues.add(allQueues.getString(i));
            }
        }
        return queues;
    }

}
