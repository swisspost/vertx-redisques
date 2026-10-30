package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.RedisReplicas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.queue.RedisService;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for a Provider for {@link RedisAPI}
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class DefaultRedisProvider implements RedisProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultRedisProvider.class);
    private final Vertx vertx;
    private final RedisquesConfigurationProvider configurationProvider;
    private RedisAPI redisAPI;
    private Redis redis;
    private RedisConnection client;

    private RedisReadyProvider readyProvider;

    private Promise<RedisAPI> connectPromise;
    private long reconnectTimer = -1;

    public DefaultRedisProvider(Vertx vertx, RedisquesConfigurationProvider configurationProvider) {
        this.vertx = vertx;
        this.configurationProvider = configurationProvider;

        maybeInitRedisReadyProvider();
    }

    private void maybeInitRedisReadyProvider() {
        RedisquesConfiguration configuration = configurationProvider.configuration();
        if (configuration.getRedisReadyCheckIntervalMs() > 0) {
            this.readyProvider = new DefaultRedisReadyProvider(vertx, configuration.getRedisReadyCheckIntervalMs());
        }
    }

    @Override
    public synchronized Future<RedisAPI> redis() {
        if(redisAPI == null) {
            return setupRedisClient();
        }
        if(readyProvider == null) {
            return Future.succeededFuture(redisAPI);
        }
        RedisAPI currentAPI = redisAPI;
        return readyProvider.ready(currentAPI).compose(ready -> {
            if (ready) {
                return Future.succeededFuture(currentAPI);
            }
            return Future.failedFuture("Not yet ready!");

        });
    }

    @Override
    public Future<RedisConnection> redisConnection() {
        return redis().compose(redisAPI -> {
            synchronized (this) {
                return client == null ? Future.failedFuture("Redis connection closed") : Future.succeededFuture(client);
            }
        });
    }

    private boolean reconnectEnabled() {
        return configurationProvider.configuration().getRedisReconnectAttempts() != 0;
    }

    private synchronized Future<RedisAPI> setupRedisClient() {
        if (connectPromise != null) {
            return connectPromise.future();
        }
        Promise<RedisAPI> currentPromise = Promise.promise();
        connectPromise = currentPromise;
        try {
            connectToRedis().onComplete(event -> {
                synchronized (this) {
                    connectPromise = null;
                    currentPromise.handle(event);
                }
            });
        } catch (RuntimeException ex) {
            connectPromise = null;
            log.warn("Failed to initialize redis client", ex);
            currentPromise.fail(ex);
        }
        return currentPromise.future();
    }

    private Future<RedisAPI> connectToRedis() {
        RedisquesConfiguration config = configurationProvider.configuration();
        String redisAuth = config.getRedisAuth();
        int redisMaxPoolSize = config.getMaxPoolSize();
        int redisMaxPoolWaitingSize = config.getMaxPoolWaitSize();
        int redisMaxPipelineWaitingSize = config.getMaxPipelineWaitSize();
        int redisPoolRecycleTimeoutMs = config.getRedisPoolRecycleTimeoutMs();
        RedisReplicas redisReplicasType = config.getRedisReplicasType();

        boolean redisConnectionTcpKeepAlive = config.getTcpKeepAlive();

        // make sure to invalidate old connection if present
        if (redis != null) {
            redis.close();
        }

        RedisOptions redisOptions = new RedisOptions()
                .setPassword((redisAuth == null ? "" : redisAuth))
                .setMaxPoolSize(redisMaxPoolSize)
                .setMaxPoolWaiting(redisMaxPoolWaitingSize)
                .setPoolRecycleTimeout(redisPoolRecycleTimeoutMs)
                .setMaxWaitingHandlers(redisMaxPipelineWaitingSize)
                .setUseReplicas(redisReplicasType)
                .setType(config.getRedisClientType());

        if (redisOptions.getType() == RedisClientType.CLUSTER) {
            // Turn on client side slots group
            RedisService.isClusterMode.compareAndSet(false, true);
        }

        NetClientOptions netClientOptions = redisOptions.getNetClientOptions();
        netClientOptions.setTcpKeepAlive(redisConnectionTcpKeepAlive);

        if (config.getRedisEnableTls()) {
            netClientOptions.setSsl(true)
                    .setHostnameVerificationAlgorithm("HTTPS");
        }

        redisOptions.setNetClientOptions(netClientOptions);

        createConnectStrings().forEach(redisOptions::addConnectionString);

        redis = Redis.createClient(vertx, redisOptions);

        return redis.connect().map(conn -> {
            synchronized (this) {
                log.info("Successfully connected to redis");
                client = conn;
                redisAPI = RedisAPI.api(conn);
                conn.exceptionHandler(ex -> {
                    log.warn("Redis connection broken", ex);
                    connectionLost(conn);
                });
                conn.endHandler(ignored -> connectionLost(conn));
                if (reconnectTimer != -1) {
                    vertx.cancelTimer(reconnectTimer);
                    reconnectTimer = -1;
                }
                return redisAPI;
            }
        });
    }

    private synchronized void connectionLost(RedisConnection connection) {
        if (client != connection || !reconnectEnabled()) {
            return;
        }
        client = null;
        redisAPI = null;
        attemptReconnect(0);
    }

    private List<String> createConnectStrings() {
        RedisquesConfiguration config = configurationProvider.configuration();
        String redisPassword = config.getRedisPassword();
        String redisUser = config.getRedisUser();
        StringBuilder connectionStringPrefixBuilder = new StringBuilder();
        connectionStringPrefixBuilder.append(config.getRedisEnableTls() ? "rediss://" : "redis://");
        if (redisUser != null && !redisUser.isEmpty()) {
            connectionStringPrefixBuilder.append(redisUser).append(":")
                    .append((redisPassword == null ? "" : redisPassword)).append("@");
        }
        List<String> connectionString = new ArrayList<>(config.getRedisHosts().size());
        String connectionStringPrefix = connectionStringPrefixBuilder.toString();
        for (int i = 0; i < config.getRedisHosts().size(); i++) {
            String host = config.getRedisHosts().get(i);
            int port = config.getRedisPorts().get(i);
            connectionString.add(connectionStringPrefix + host + ":" + port);
        }
        return connectionString;
    }

    private synchronized void attemptReconnect(int retry) {
        if (client != null) {
            return;
        }
        log.info("About to reconnect to redis with attempt #{}", retry);
        int reconnectAttempts = configurationProvider.configuration().getRedisReconnectAttempts();
        if (reconnectAttempts < 0) {
            doReconnect(retry);
        } else if (retry >= reconnectAttempts) {
            log.warn("Not reconnecting anymore since max reconnect attempts ({}) are reached", reconnectAttempts);
        } else {
            doReconnect(retry);
        }
    }

    private void doReconnect(int retry) {
        long configDelayMs = configurationProvider.configuration().getRedisReconnectDelaySec() * 1000L;
        long backoffMs = (long) (Math.pow(2, Math.min(retry, 10)) * configDelayMs);
        log.debug("Schedule reconnect #{} in {}ms.", retry, backoffMs);
        if (reconnectTimer != -1) {
            return;
        }
        reconnectTimer = vertx.setTimer(backoffMs, timer -> {
            synchronized (this) {
                if (reconnectTimer != timer) {
                    return;
                }
                reconnectTimer = -1;
                setupRedisClient().onFailure(ex -> {
                    log.info("Reconnect failed. Try again.", ex);
                    attemptReconnect(retry + 1);
                });
            }
        });
    }

}
