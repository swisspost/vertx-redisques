package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;

public interface RedisquesConfigurationProvider {

    RedisquesConfiguration configuration();

    Result<Void, String> updateConfiguration(JsonObject configuration, boolean validateOnly);
}
