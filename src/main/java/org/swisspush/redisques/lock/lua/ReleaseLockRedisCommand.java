package org.swisspush.redisques.lock.lua;

import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.lua.LuaScriptState;
import org.swisspush.redisques.lua.RedisCommand;
import org.swisspush.redisques.queue.RedisService;


import java.util.ArrayList;
import java.util.List;

/**
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class ReleaseLockRedisCommand implements RedisCommand {

    private LuaScriptState luaScriptState;
    private List<String> keys;
    private List<String> arguments;
    private Promise<Boolean> promise;
    private RedisService redisService;
    private final RedisQuesExceptionFactory exceptionFactory;
    private Logger log;

    public ReleaseLockRedisCommand(
            LuaScriptState luaScriptState,
            List<String> keys,
            List<String> arguments,
            RedisService redisService,
            RedisQuesExceptionFactory exceptionFactory,
            Logger log,
            final Promise<Boolean> promise
    ) {
        this.luaScriptState = luaScriptState;
        this.keys = keys;
        this.arguments = arguments;
        this.redisService = redisService;
        this.exceptionFactory = exceptionFactory;
        this.log = log;
        this.promise = promise;
    }

    @Override
    public void exec(int executionCounter) {
        List<String> args = new ArrayList<>();
        args.add(luaScriptState.getSha());
        args.add(String.valueOf(keys.size()));
        args.addAll(keys);
        args.addAll(arguments);
        redisService.evalsha(args).onComplete(event -> {
            if (event.succeeded()) {
                Long unlocked = event.result().toLong();
                promise.complete(unlocked > 0);
            } else {
                Throwable ex = event.cause();
                String message = ex.getMessage();
                if (message != null && message.startsWith("NOSCRIPT")) {
                    log.warn("ReleaseLockRedisCommand script couldn't be found, reload it",
                            exceptionFactory.newException(ex));
                    log.warn("amount the script got loaded: {}", executionCounter);
                    if (executionCounter > 10) {
                        promise.fail(exceptionFactory.newException("amount the script got loaded is higher than 10, we abort"));
                    } else {
                        luaScriptState.loadLuaScript(new ReleaseLockRedisCommand(luaScriptState, keys,
                                arguments, redisService, exceptionFactory, log, promise), executionCounter);
                    }
                } else {
                    promise.fail(exceptionFactory.newException("ReleaseLockRedisCommand request failed", ex));
                }
            }
        });
    }
}
