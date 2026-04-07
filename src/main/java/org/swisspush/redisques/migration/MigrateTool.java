package org.swisspush.redisques.migration;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.migration.tasks.QueueConsumersMigrationTask;
import org.swisspush.redisques.migration.tasks.Task;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.System.currentTimeMillis;

public class MigrateTool {
    private static final Logger log = LoggerFactory.getLogger(MigrateTool.class);
    private static final int migrationLockTimeout = 30_000; // 30 seconds
    private static final int migrationLockUpdateTime = 10_000; // 10 second
    private final String uid;
    private final RedisService redisService;
    private final RedisquesConfigurationProvider redisquesConfigurationProvider;
    private final KeyspaceHelper keyspaceHelper;
    private final Vertx vertx;
    private final List<Task> tasks = new ArrayList<>();
    private Long refreshTimerId = null;

    public MigrateTool(Vertx vertx, RedisquesConfigurationProvider configurationProvider, RedisService redisService, KeyspaceHelper keyspaceHelper, String uid) {
        this.redisquesConfigurationProvider = configurationProvider;
        this.redisService = redisService;
        this.keyspaceHelper = keyspaceHelper;
        this.vertx = vertx;
        this.uid = uid;
        // add all tasks, will run each one as it ordered.
        tasks.add(new QueueConsumersMigrationTask(vertx, redisService));
    }


    public Future<Void> start() {
        final Promise<Void> migrateRunnerPromise = Promise.promise();
        redisService.setNxPx(keyspaceHelper.getDataMigrationLockKey(), String.valueOf(currentTimeMillis()), true, migrationLockTimeout).onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                log.error("Failed to set lock for migration. (uid:{})", uid);
                migrateRunnerPromise.fail(asyncResult.cause());
                return;
            }
            if (asyncResult.result()) {
                startRefreshTimer();
                log.info("migration runner started. (uid:{})", uid);
                vertx.setTimer(50000, new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        migrateTasksDone();
                    }
                });


            } else {
                log.info("Another migration runner already started. (uid:{})", uid);
                waitUntilOtherMigrationDone().onComplete(migrateResult -> {
                    log.info("migration runner from another instance completed. (uid:{})", uid);
                    migrateRunnerPromise.complete();
                });
            }
        });

        return migrateRunnerPromise.future();
    }


    public Future<Void> waitUntilOtherMigrationDone() {
        Promise<Void> promise = Promise.promise();
        checkKey(promise);
        return promise.future();
    }

    private Future<Void> migrateTasksDone() {
        stopRefreshTimer();
        return redisService.del(Collections.singletonList(keyspaceHelper.getDataMigrationLockKey())).compose(response -> Future.succeededFuture());
    }

    private void checkKey(final Promise<Void> promise) {
        log.debug("check does migration tasks done. (uid:{})", uid);
        redisService.exists(Collections.singletonList(keyspaceHelper.getDataMigrationLockKey())).onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                log.error("Failed to check migration lock.");
                promise.fail(asyncResult.cause());
                return;
            }
            Response res = asyncResult.result();
            int exists = res.toInteger();
            if (exists == 0) {
                promise.complete();
            } else {
                // Key still exists → check again later
                Vertx.currentContext().owner().setTimer(migrationLockUpdateTime, id -> {
                    checkKey(promise);
                });
            }
        });
    }

    private void startRefreshTimer() {
        refreshTimerId = vertx.setPeriodic(migrationLockUpdateTime, event -> {
            redisService.setPExpire(keyspaceHelper.getDataMigrationLockKey(), migrationLockTimeout).onComplete(asyncResult -> {
                if (asyncResult.failed()) {
                    log.error("Failed to update migration lock.");
                } else {
                    log.debug("Migration lock TTL updated (uid:{})", uid);
                }
            });
        });
    }

    private void stopRefreshTimer() {
        if (refreshTimerId != null) {
            vertx.cancelTimer(refreshTimerId);
            log.debug("Migration lock update stopped (uid:{})", uid);
        }
    }
}
