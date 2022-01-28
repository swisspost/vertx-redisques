package org.swisspush.redisques.lua;

import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.client.RedisAPI;
import org.swisspush.redisques.util.RedisUtils;

import java.util.*;

/**
 * Manages the lua scripts.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class LuaScriptManager {

    private RedisAPI redisAPI;
    private Map<LuaScript,LuaScriptState> luaScripts = new HashMap<>();
    private Logger log = LoggerFactory.getLogger(LuaScriptManager.class);

    public LuaScriptManager(RedisAPI redisAPI){
        this.redisAPI = redisAPI;

        LuaScriptState luaGetScriptState = new LuaScriptState(LuaScript.CHECK, redisAPI);
        luaGetScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.CHECK, luaGetScriptState);
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
        executeRedisCommand(new Check(keys, arguments, redisAPI, handler), 0);
    }

    private class Check implements RedisCommand {

        private List<String> keys;
        private List<String> arguments;
        private Handler<Boolean> handler;
        private RedisAPI redisAPI;

        public Check(List<String> keys, List<String> arguments, RedisAPI redisAPI, final Handler<Boolean> handler) {
            this.keys = keys;
            this.arguments = arguments;
            this.redisAPI = redisAPI;
            this.handler = handler;
        }

        @Override
        public void exec(int executionCounter) {
            List<String> args= RedisUtils.toPayload(luaScripts.get(LuaScript.CHECK).getSha(), keys.size(), keys, arguments);
            redisAPI.evalsha(args, event -> {
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
                        log.warn("amount the script got loaded: " + String.valueOf(executionCounter));
                        if(executionCounter > 10) {
                            log.error("amount the script got loaded is higher than 10, we abort");
                        } else {
                            luaScripts.get(LuaScript.CHECK).loadLuaScript(new Check(keys, arguments, redisAPI, handler), executionCounter);
                        }
                    } else {
                        log.error("Check request failed.", event.cause());
                    }
                }
            });
        }
    }
}
