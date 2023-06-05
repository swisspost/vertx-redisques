package org.swisspush.redisques.util;

import io.vertx.core.*;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation for a Provider for {@link RedisAPI}
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class DefaultRedisProvider implements RedisProvider {

    private final Vertx vertx;
    private RedisquesConfigurationProvider configurationProvider;

    private RedisAPI redisAPI;

    private final AtomicReference<Promise<RedisAPI>> connectPromiseRef = new AtomicReference<>();

    public DefaultRedisProvider(Vertx vertx, RedisquesConfigurationProvider configurationProvider) {
        this.vertx = vertx;
        this.configurationProvider = configurationProvider;
    }

    @Override
    public Future<RedisAPI> redis() {
        if(redisAPI != null) {
            return Future.succeededFuture(redisAPI);
        } else {
            return setupRedisClient();
        }
    }

    private Future<RedisAPI> setupRedisClient(){
        Promise<RedisAPI> currentPromise = Promise.promise();
        Promise<RedisAPI> masterPromise = connectPromiseRef.accumulateAndGet(
                currentPromise, (oldVal, newVal) -> (oldVal != null) ? oldVal : newVal);
        if( currentPromise == masterPromise ){
            // Our promise is THE promise. So WE have to resolve it.
            connectToRedis().onComplete(event -> {
                if(event.failed()) {
                    currentPromise.fail(event.cause());
                } else {
                    connectPromiseRef.getAndSet(null);
                    redisAPI = event.result();
                    currentPromise.complete(redisAPI);
                }
            });
        }

        // Always return master promise (even if we didn't create it ourselves)
        return masterPromise.future();
    }

    private Future<RedisAPI> connectToRedis() {
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