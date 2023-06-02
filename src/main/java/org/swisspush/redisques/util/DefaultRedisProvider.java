package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;

import java.util.function.Function;

/**
 * Default implementation for a Provider for {@link RedisAPI}
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class DefaultRedisProvider implements RedisProvider {

    private final Vertx vertx;
    private RedisquesConfigurationProvider configurationProvider;

    private RedisAPI redisAPI;

    public DefaultRedisProvider(Vertx vertx, RedisquesConfigurationProvider configurationProvider) {
        this.vertx = vertx;
        this.configurationProvider = configurationProvider;
        setupRedisAPI().onComplete(event -> {
            if(event.succeeded()) {
                redisAPI = event.result();
            } else {
                redisAPI = null;
            }
        });
    }

    @Override
    public Future<RedisAPI> redis() {
        if(redisAPI != null) {
            return Future.succeededFuture(redisAPI);
        } else {
            return Future.failedFuture("Not yet ready");
        }
    }

    public Future<Void> initialize() {
        return setupRedisAPI().compose(redisAPI -> Future.succeededFuture());
    }

    private Future<RedisAPI> setupRedisAPI() {
        RedisquesConfiguration config = configurationProvider.configuration();
        String redisHost = config.getRedisHost();
        int redisPort = config.getRedisPort();
        String redisAuth = config.getRedisAuth();
        int redisMaxPoolSize = config.getMaxPoolSize();
        int redisMaxPoolWaitingSize = config.getMaxPoolWaitSize();
        int redisMaxPipelineWaitingSize = config.getMaxPipelineWaitSize();

        Promise<RedisAPI> promise = Promise.promise();
        Redis.createClient(vertx, new RedisOptions()
                .setConnectionString("redis://" + redisHost + ":" + redisPort)
                .setPassword((redisAuth == null ? "" : redisAuth))
                .setMaxPoolSize(redisMaxPoolSize)
                .setMaxPoolWaiting(redisMaxPoolWaitingSize)
                .setMaxWaitingHandlers(redisMaxPipelineWaitingSize)
        ).connect(event -> {
            if (event.failed()) {
                promise.fail(event.cause());
            } else {
                promise.complete(RedisAPI.api(event.result()));
            }
        });

        return promise.future();
    }
}
