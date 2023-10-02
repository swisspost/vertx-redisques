package org.swisspush.redisques.util;

import io.netty.util.internal.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.stream.Stream;

/**
 * Useful utilities for Redis
 *
 */
public final class RedisUtils {

    private RedisUtils() {}

    /**
     * from https://github.com/vert-x3/vertx-redis-client/blob/3.9/src/main/java/io/vertx/redis/impl/RedisClientImpl.java#L94
     *
     * @param parameters
     * @return
     */
    public static List<String> toPayload(Object... parameters) {
        List<String> result = new ArrayList<>(parameters.length);

        for (Object param : parameters) {
            // unwrap
            if (param instanceof JsonArray) {
                param = ((JsonArray) param).getList();
            }
            // unwrap
            if (param instanceof JsonObject) {
                param = ((JsonObject) param).getMap();
            }

            if (param instanceof Collection) {
                ((Collection) param).stream().filter(Objects::nonNull).forEach(o -> result.add(o.toString()));
            } else if (param instanceof Map) {
                for (Map.Entry<?, ?> pair : ((Map<?, ?>) param).entrySet()) {
                    result.add(pair.getKey().toString());
                    result.add(pair.getValue().toString());
                }
            } else if (param instanceof Stream) {
                ((Stream) param).forEach(e -> {
                    if (e instanceof Object[]) {
                        Collections.addAll(result, (String[]) e);
                    } else {
                        result.add(e.toString());
                    }
                });
            } else if (param != null) {
                result.add(param.toString());
            }
        }
        return result;
    }
    public static String formatAsHastag(String queueName) {
        if (StringUtil.isNullOrEmpty(queueName)){
            return queueName;
        }
        return "{" + queueName + "}";
    }
}
