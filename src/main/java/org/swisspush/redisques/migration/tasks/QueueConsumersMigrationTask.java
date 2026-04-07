package org.swisspush.redisques.migration.tasks;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import org.swisspush.redisques.queue.RedisService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class QueueConsumersMigrationTask implements Task {

    private final Vertx vertx;

    public QueueConsumersMigrationTask(Vertx vertx, RedisService redisService) {
        this.vertx = vertx;
    }

    @Override
    public String getTaskKey() {
        return "QueueConsumersMigrationTask";
    }

    @Override
    public Future<Void> run() {
        return null;
    }

}
