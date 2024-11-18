package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

import java.util.List;

/**
 * Provider for {@link RedisAPI}
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public interface RedisProvider {
    Future<RedisAPI> redis();
    Future<List<Response>> execBatchCommand(List<Request> commands);
}
