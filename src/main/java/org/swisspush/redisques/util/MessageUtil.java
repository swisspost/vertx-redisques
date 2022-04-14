package org.swisspush.redisques.util;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.swisspush.redisques.util.RedisquesAPI.FILTER;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;

/**
 * Util class to work with {@link Message}s
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class MessageUtil {

    private static final Logger log = LoggerFactory.getLogger(MessageUtil.class);

    public static Result<Optional<Pattern>, String> extractFilterPattern(Message<JsonObject> event) {
        JsonObject payload = event.body().getJsonObject(PAYLOAD);
        if (payload == null || payload.getString(FILTER) == null) {
            return Result.ok(Optional.empty());
        }
        String filterString = payload.getString(FILTER);
        try {
            Pattern pattern = Pattern.compile(filterString);
            return Result.ok(Optional.of(pattern));
        } catch (Exception ex) {
            log.error("Interface doesn't allow to pass stack trace. Therefore simply log it now.", ex);
            return Result.err("Error while compile regex pattern. Cause: " + ex.getMessage());
        }
    }
}
