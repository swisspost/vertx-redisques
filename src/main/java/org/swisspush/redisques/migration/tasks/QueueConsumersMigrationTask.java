package org.swisspush.redisques.migration.tasks;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.QueueRegistryService;
import org.swisspush.redisques.queue.RedisService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueueConsumersMigrationTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(QueueConsumersMigrationTask.class);
    private final Vertx vertx;
    private final RedisService redisService;
    private final KeyspaceHelper keyspaceHelper;

    public QueueConsumersMigrationTask(Vertx vertx, RedisService redisService, KeyspaceHelper keyspaceHelper) {
        this.vertx = vertx;
        this.redisService = redisService;
        this.keyspaceHelper = keyspaceHelper;
    }

    @Override
    public String getTaskKey() {
        return "QueueConsumersMigrationTask";
    }

    @Override
    public Future<Boolean> run() {
        Promise<Boolean> promise = Promise.promise();
        getMigrationFlag().onComplete(event -> {
            if (event.failed()) {
                log.error("Error while running QueueConsumersMigrationTask, can't check state", event.cause());
                promise.fail(event.cause());
            } else {
                String value = event.result();
                if (value != null) {
                    log.info("{} has been migrated at {}, will skip", getTaskKey(), value);
                    promise.complete(false);
                } else {
                    log.info("{} already migrated at {}, will skip", getTaskKey(), System.currentTimeMillis());
                    doMigration().onComplete(event1 -> {
                        if (event1.failed()) {
                            log.error("miration failed: {}", event1.cause().getMessage());
                            promise.fail(event1.cause());
                        } else {
                            setMigrationFlag().onComplete(event2 -> {
                                if (event2.failed()) {
                                    promise.fail(event2.cause());
                                }else {
                                    promise.complete(true);
                                }
                            });
                        }
                    });
                }
            }
        });
        return promise.future();
    }

    private Future<Void> doMigration() {
        final Promise<Void> promise = Promise.promise();
        redisService.scanCluster(keyspaceHelper.getConsumersPrefix() + "*").onComplete(ar -> {
            if (ar.failed()) {
                log.error("failed to get old data from redis by scan: {}", ar.cause().getMessage());
                promise.fail(ar.cause());
            } else {
                List<Map<String, String>> oldConsumersData = QueueRegistryService.splitMap(ar.result(), RedisService.MAX_COMMANDS_IN_BATCH);
                List<Request> batch = new ArrayList<>();
                for (Map<String, String> map : oldConsumersData) {
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        String oldkey = entry.getKey();
                        oldkey = oldkey.replace(keyspaceHelper.getConsumersPrefix(), "");
                        String newKey = keyspaceHelper.getClusterSafeConsumersPrefix() + oldkey;
                        batch.add(Request.cmd(Command.SET)
                                .arg(newKey)
                                .arg(entry.getValue()));
                    }
                }
                if (batch.isEmpty()) {
                    promise.complete();
                    return;
                }
                redisService.batch(batch).onComplete(event -> {
                    if (event.failed()) {
                        promise.fail(event.cause());
                    } else {
                        promise.complete();
                    }
                });
            }
        });
        return promise.future();
    }

    private Future<String> getMigrationFlag() {
        Promise<String> promise = Promise.promise();
        log.info("Checking QueueConsumersMigrationTask migration flag...");
        redisService.get(keyspaceHelper.getDataMigrationTaskPrefix() + getTaskKey()).onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                log.error("Checking QueueConsumersMigrationTask migration flag failed");
                promise.fail(asyncResult.cause());
            } else {
                log.debug("Checking QueueConsumersMigrationTask migration flag succeeded");
                if (asyncResult.result() != null) {
                    promise.complete(asyncResult.result().toString());
                } else {
                    promise.complete(null);
                }
            }
        });
        return promise.future();
    }

    private Future<Void> setMigrationFlag() {
        Promise<Void> promise = Promise.promise();
        log.info("Set QueueConsumersMigrationTask migration flag...");
        redisService.set(keyspaceHelper.getDataMigrationTaskPrefix() + getTaskKey(), String.valueOf(System.currentTimeMillis()))
                .onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                log.error("Set QueueConsumersMigrationTask migration flag failed");
                promise.fail(asyncResult.cause());
            } else {
                log.debug("Set QueueConsumersMigrationTask migration flag succeeded");
                promise.complete(null);
            }
        });
        return promise.future();
    }
}
