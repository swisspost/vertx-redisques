package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

/**
 * Provider for {@link RedisAPI}
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public interface RedisProvider {
    Future<RedisAPI> redis();
}
