package org.swisspush.redisques.util;

import io.vertx.redis.client.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class HandlerUtil {

    /**
     * Apply the given filter pattern to the given JsonArray containing the list of queues/locks.
     * @param response list of JSON objects containing the queueName/lockName to be filtered
     * @param filterPattern The filter regex pattern to be matched against the given queues/locks list
     * @return The resulting filtered list of queues/locks. If there is no filter given, the full list
     *         of queues/locks is returned.
     */
    public static List<String> filterByPattern(Response response, Optional<Pattern> filterPattern) {
        List<String> queues = new ArrayList<>();
        if (filterPattern != null && filterPattern.isPresent()) {
            Pattern pattern = filterPattern.get();
            for (int i = 0; i < response.size(); i++) {
                String queue = response.get(i).toString();
                if (pattern.matcher(queue).find()) {
                    queues.add(queue);
                }
            }
        } else {
            for (int i = 0; i < response.size(); i++) {
                queues.add(response.get(i).toString());
            }
        }
        return queues;
    }

}
