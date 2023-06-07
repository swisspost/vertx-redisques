package org.swisspush.redisques.lua;

import io.vertx.core.Handler;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisUtils;

import java.util.*;

/**
 * Manages the lua scripts.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class LuaScriptManager {

    private RedisProvider redisProvider;
    private Map<LuaScript,LuaScriptState> luaScripts = new HashMap<>();
    private Logger log = LoggerFactory.getLogger(LuaScriptManager.class);

    public LuaScriptManager(RedisProvider redisProvider){
        this.redisProvider = redisProvider;

        LuaScriptState luaGetScriptState = new LuaScriptState(LuaScript.CHECK, redisProvider);
        luaGetScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.CHECK, luaGetScriptState);

        // load the MultiListLength Lua Script
        LuaScriptState luaMllenScriptState = new LuaScriptState(LuaScript.MLLEN, redisProvider);
        luaMllenScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.MLLEN, luaMllenScriptState);
    }

    /**
     * If the loglevel is trace and the logoutput in luaScriptState is false, then reload the script with logoutput and execute the RedisCommand.
     * If the loglevel is not trace and the logoutput in luaScriptState is true, then reload the script without logoutput and execute the RedisCommand.
     * If the loglevel is matching the luaScriptState, just execute the RedisCommand.
     *
     * @param redisCommand the redis command to execute
     * @param executionCounter current count of already passed executions
     */
    private void executeRedisCommand(RedisCommand redisCommand, int executionCounter) {
        redisCommand.exec(executionCounter);
    }

    public void handleQueueCheck(String lastCheckExecKey, int checkInterval, Handler<Boolean> handler){
        List<String> keys = Collections.singletonList(lastCheckExecKey);
        List<String> arguments = Arrays.asList(
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(checkInterval)
        );
        executeRedisCommand(new Check(keys, arguments, redisProvider, handler), 0);
    }

    private class Check implements RedisCommand {

        private List<String> keys;
        private List<String> arguments;
        private Handler<Boolean> handler;
        private RedisProvider redisProvider;

        public Check(List<String> keys, List<String> arguments, RedisProvider redisProvider, final Handler<Boolean> handler) {
            this.keys = keys;
            this.arguments = arguments;
            this.redisProvider = redisProvider;
            this.handler = handler;
        }

        @Override
        public void exec(int executionCounter) {
            List<String> args= RedisUtils.toPayload(luaScripts.get(LuaScript.CHECK).getSha(), keys.size(), keys, arguments);
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.evalsha(args, event -> {
                if(event.succeeded()){
                    Long value = event.result().toLong();
                    if (log.isTraceEnabled()) {
                        log.trace("Check lua script got result: " + value);
                    }
                    handler.handle(value == 1L);
                } else {
                    String message = event.cause().getMessage();
                    if(message != null && message.startsWith("NOSCRIPT")) {
                        log.warn("Check script couldn't be found, reload it");
                        log.warn("amount the script got loaded: " + executionCounter);
                        if(executionCounter > 10) {
                            log.error("amount the script got loaded is higher than 10, we abort");
                        } else {
                            luaScripts.get(LuaScript.CHECK).loadLuaScript(new Check(keys, arguments, redisProvider, handler), executionCounter);
                        }
                    } else {
                        log.error("Check request failed.", event.cause());
                    }
                }
            })).onFailure(throwable -> log.error("Redis: Check request failed.", throwable));
        }
    }


    public void handleMultiListLength(List<String> keys, Handler<List<Long>> handler){
        executeRedisCommand(new MultiListLength(keys, redisProvider, handler), 0);
    }

    private class MultiListLength implements RedisCommand {

        private List<String> keys;
        private Handler<List<Long>> handler;
        private RedisProvider redisProvider;

        public MultiListLength(List<String> keys, RedisProvider redisProvider, final Handler<List<Long>> handler) {
            this.keys = keys;
            this.redisProvider = redisProvider;
            this.handler = handler;
        }

        @Override
        public void exec(int executionCounter) {
            if (keys==null || keys.isEmpty()){
                handler.handle(List.of());
                return;
            }
            List<String> args= RedisUtils.toPayload(luaScripts.get(LuaScript.MLLEN).getSha(),
                keys.size(), keys);
            redisProvider.redis().onSuccess(redisAPI -> redisAPI.evalsha(args, event -> {
                if(event.succeeded()){
                    List<Long> res = new ArrayList<>();
                    for (Response response : event.result()) {
                        res.add(response.toLong());
                    }
                    handler.handle(res);
                } else {
                    String message = event.cause().getMessage();
                    if(message != null && message.startsWith("NOSCRIPT")) {
                        log.warn("MultiListLength script couldn't be found, reload it");
                        log.warn("amount the script got loaded: {}", executionCounter);
                        if(executionCounter > 10) {
                            log.error("amount the MultiListLength script got loaded is higher than 10, we abort");
                        } else {
                            luaScripts.get(LuaScript.MLLEN).loadLuaScript(new MultiListLength(keys, redisProvider, handler), executionCounter);
                        }
                    } else {
                        log.error("ListLength request failed.", event.cause());
                    }
                    event.failed();
                }
            })).onFailure(throwable -> log.error("Redis: ListLength request failed.", throwable));
        }
    }


}
