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
                connectPromiseRef.getAndSet(null);
                if(event.failed()) {
                    currentPromise.fail(event.cause());
                } else {
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
        String redisAuth = config.getRedisAuth();
        int redisMaxPoolSize = config.getMaxPoolSize();
        int redisMaxPoolWaitingSize = config.getMaxPoolWaitSize();
        int redisMaxPipelineWaitingSize = config.getMaxPipelineWaitSize();

        Promise<RedisAPI> promise = Promise.promise();
        RedisOptions redisOptions = new RedisOptions()
                .setConnectionString(createConnectString())
                .setMaxPoolSize(redisMaxPoolSize)
                .setMaxPoolWaiting(redisMaxPoolWaitingSize)
                .setMaxWaitingHandlers(redisMaxPipelineWaitingSize);
        redisOptions.setPassword((redisAuth == null ? "" : redisAuth));
        Redis.createClient(vertx, redisOptions).connect(event -> {
            if (event.failed()) {
                promise.fail(event.cause());
            } else {
                promise.complete(RedisAPI.api(event.result()));
            }
        });

        return promise.future();
    }

    private String createConnectString() {
        RedisquesConfiguration config = configurationProvider.configuration();
        String redisPassword = config.getRedisPassword();
        String redisUser = config.getRedisUser();
        StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append(config.getRedisEnableTls() ? "rediss://" : "redis://");
        if (redisUser != null && !redisUser.isEmpty()) {
            connectionStringBuilder.append(redisUser).append(":").append((redisPassword == null ? "" : redisPassword)).append("@");
        }
        connectionStringBuilder.append(config.getRedisHost()).append(":").append(config.getRedisPort());
        return connectionStringBuilder.toString();
    }
}
