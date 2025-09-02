package org.swisspush.redisques.lock.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.lock.Lock;
import org.swisspush.redisques.lock.lua.LockLuaScripts;
import org.swisspush.redisques.lua.LuaScriptState;
import org.swisspush.redisques.lock.lua.ReleaseLockRedisCommand;
import org.swisspush.redisques.queue.RedisService;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of the {@link Lock} interface based on a redis database.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisBasedLock implements Lock {

    private static final Logger log = LoggerFactory.getLogger(RedisBasedLock.class);
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    public static final String STORAGE_PREFIX = "gateleen.core-lock:";

    private final LuaScriptState releaseLockLuaScriptState;
    private final RedisService redisService;
    private final RedisQuesExceptionFactory exceptionFactory;

    public RedisBasedLock(RedisService redisService, RedisQuesExceptionFactory exceptionFactory) {
        this.redisService = redisService;
        this.exceptionFactory = exceptionFactory;
        this.releaseLockLuaScriptState = new LuaScriptState(LockLuaScripts.LOCK_RELEASE, redisService, exceptionFactory, false);
    }


    @Override
    public Future<Boolean> acquireLock(String lock, String token, long lockExpiryMs) {
        Promise<Boolean> promise = Promise.promise();
        String lockKey = buildLockKey(lock);
        redisService.setNxPx(lockKey, token, true, lockExpiryMs).onComplete(event -> {
            if (event.succeeded()) {
                promise.complete(event.result());
            } else {
                Throwable ex = exceptionFactory.newException(
                        "redisSetWithOptions(lockKey=\"" + lockKey + "\") failed", event.cause());
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Boolean> releaseLock(String lock, String token) {
        Promise<Boolean> promise = Promise.promise();
        List<String> keys = Collections.singletonList(buildLockKey(lock));
        List<String> arguments = Collections.singletonList(token);
        ReleaseLockRedisCommand cmd = new ReleaseLockRedisCommand(releaseLockLuaScriptState,
                keys, arguments, redisService, exceptionFactory, log, promise);
        cmd.exec(0);
        return promise.future();
    }

    private String buildLockKey(String lock) {
        return STORAGE_PREFIX + lock;
    }
}
