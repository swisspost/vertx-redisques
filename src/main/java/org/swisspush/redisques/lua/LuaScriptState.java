package org.swisspush.redisques.lua;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Holds the state of a lua script.
 */
public class LuaScriptState {

    private LuaScript luaScriptType;
    /** the script itself */
    private String script;
    /** the sha, over which the script can be accessed in redis */
    private String sha;

    private RedisClient redisClient;

    private Logger log = LoggerFactory.getLogger(LuaScriptState.class);

    public LuaScriptState(LuaScript luaScriptType, RedisClient redisClient) {
        this.luaScriptType = luaScriptType;
        this.redisClient = redisClient;
        this.composeLuaScript(luaScriptType);
        this.loadLuaScript(new RedisCommandDoNothing(), 0);
    }

    /**
     * Reads the script from the classpath and removes logging output if logoutput is false.
     * The script is stored in the class member script.
     * @param luaScriptType the lua script type
     */
    private void composeLuaScript(LuaScript luaScriptType) {
        log.info("read the lua script for script type: " + luaScriptType);
        this.script = readLuaScriptFromClasspath(luaScriptType);
        this.sha = DigestUtils.sha1Hex(this.script);
    }

    private String readLuaScriptFromClasspath(LuaScript luaScriptType) {
        BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(luaScriptType.getFile())));
        StringBuilder sb;
        try {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                sb.append(line).append("\n");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // Ignore
            }
        }
        return sb.toString();
    }

    /**
     * Rereads the lua script, eg. if the loglevel changed.
     */
    public void recomposeLuaScript() {
        this.composeLuaScript(luaScriptType);
    }

    /**
     * Load the get script into redis and store the sha in the class member sha.
     * @param redisCommand the redis command that should be executed, after the script is loaded.
     * @param executionCounter a counter to control recursion depth
     */
    public void loadLuaScript(final RedisCommand redisCommand, int executionCounter) {
        final int executionCounterIncr = ++executionCounter;

        // check first if the lua script already exists in the store
        redisClient.scriptExists(this.sha, resultArray -> {
            if(resultArray.failed()){
                log.error("Error checking whether lua script exists", resultArray.cause());
                return;
            }
            Long exists = resultArray.result().getLong(0);
            // if script already
            if(Long.valueOf(1).equals(exists)) {
                log.debug("RedisStorage script already exists in redis cache: " + luaScriptType);
                redisCommand.exec(executionCounterIncr);
            } else {
                log.info("load lua script for script type: " + luaScriptType);
                redisClient.scriptLoad(script, stringAsyncResult -> {
                    if (stringAsyncResult.failed()) {
                        log.warn("Received failed message for loadLuaScript. Lets run into NullPointerException now.", stringAsyncResult.cause());
                        // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
                    }
                    String newSha = stringAsyncResult.result();
                    log.info("got sha from redis for lua script: " + luaScriptType + ": " + newSha);
                    if(!newSha.equals(sha)) {
                        log.warn("the sha calculated by myself: " + sha + " doesn't match with the sha from redis: " + newSha + ". We use the sha from redis");
                    }
                    sha = newSha;
                    log.info("execute redis command for script type: " + luaScriptType + " with new sha: " + sha);
                    redisCommand.exec(executionCounterIncr);
                });
            }
        });
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public String getSha() {
        return sha;
    }

    public void setSha(String sha) {
        this.sha = sha;
    }
}
