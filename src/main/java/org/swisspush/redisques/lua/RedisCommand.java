package org.swisspush.redisques.lua;

/**
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public interface RedisCommand {
    void exec(int executionCounter);
}
