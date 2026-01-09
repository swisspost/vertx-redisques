package org.swisspush.redisques.queue;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisUtils;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class RedisService {
    private final RedisProvider redisProvider;
    private static final Logger log = LoggerFactory.getLogger(RedisService.class);

    public RedisService(RedisProvider redisProvider) {
        this.redisProvider = redisProvider;
    }

    private Future<RedisAPI> redis() {
        return redisProvider.redis().onFailure(
                throwable -> log.warn("Redis: Failed to get RedisAPI", throwable)
        ).compose(Future::succeededFuture);
    }

    public Future<Response> script(String name, String sha) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.script(Arrays.asList(name, sha))
        );
    }

    public Future<Response> evalsha(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.evalsha(args)
        );
    }

    public Future<Response> info(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.info(args)
        );
    }

    public Future<Response> lpop(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lpop(args)
        );
    }

    public Future<Response> exists(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.exists(args)
        );
    }

    public Future<Response> del(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.del(args)
        );
    }

    public Future<Response> hmset(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hmset(args)
        );
    }

    public Future<Response> get(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.get(key)
        );
    }

    public Future<Boolean> setNxPx(String key, String value, boolean setNx, long durationMillis) {
        return redis().compose((RedisAPI redisAPI) -> {
            String durationMillisStr = String.valueOf(durationMillis);
            if (setNx) {
                return redisAPI.send(Command.SET, key, value, "NX", "PX", durationMillisStr);
            } else {
                return redisAPI.send(Command.SET, key, value, "PX", durationMillisStr);
            }
        }).map((Response rsp) -> {
            return rsp != null && "OK".equals(rsp.toString());
        });
    }

    public Future<Response> zadd(String key, String value, String score) {
        return zadd(key, List.of(), value, score);
    }

    public Future<Response> zadd(String key, List<String> params, String value, String score) {
        return redis().compose((RedisAPI redisAPI) -> {
                    if (params.isEmpty()) {
                        return redisAPI.zadd(Arrays.asList(key, score, value));
                    } else {
                        List<String> cmdParams = new ArrayList<>();
                        cmdParams.add(key);
                        cmdParams.addAll(params);
                        cmdParams.add(score);
                        cmdParams.add(value);
                        return redisAPI.zadd(cmdParams);
                    }
                }
        );
    }

    public Future<Response> zscore(String key, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zscore(key, value)
        );
    }

    public Future<Response> zrem(String key, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zrem(Arrays.asList(key, value))
        );
    }

    public Future<Response> hset(String queuesKey, String queueName, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hset(Arrays.asList(queuesKey, queueName, value))
        );
    }

    public Future<Response> hdel(String key, String queueName) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hdel(Arrays.asList(key, queueName))
        );
    }

    public Future<Response> hdel(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hdel(args)
        );
    }

    public Future<Response> llen(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.llen(key)
        );
    }

    public Future<Response> hvals(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hvals(key)
        );
    }

    public Future<Response> hget(String key, String field) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hget(key, field)
        );
    }

    public Future<Response> ltrim(String key, String start, String stop) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.ltrim(key, start, stop)
        );
    }

    public Future<Response> lrange(String key, String start, String stop) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lrange(key, start, stop)
        );
    }

    public Future<Response> hexists(String key, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hexists(key, value)
        );
    }

    public Future<Response> lindex(String key, String index) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lindex(key, index)
        );
    }

    public Future<Response> lset(String key, String index, String element) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lset(key, index, element)
        );
    }

    public Future<Response> lrem(String key, String count, String element) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lrem(key, count, element)
        );
    }

    public Future<Response> zremrangebyscore(String key, String min, String max) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zremrangebyscore(key, min, max)
        );
    }

    public Future<Response> zrangebyscore(String key, String min, String max) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zrangebyscore(Arrays.asList(key, min, max))
        );
    }

    public Future<Response> zcount(String key, String min, String max) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zcount(key, min, max)
        );
    }

    public Future<Response> rpush(String key, String item) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.rpush(Arrays.asList(key, item))
        );
    }

    public Future<Response> hkeys(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hkeys(key)
        );
    }

    public Future<Response> keys(String pattern) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.keys(pattern)
        );
    }

    public Future<Response> expire(String key, String seconds) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.expire(Arrays.asList(key, seconds))
        );
    }

    public Future<Response> scan(String cursor, @Nullable String pattern, @Nullable String count, @Nullable String type) {
        List<String> args = new ArrayList<>();
        args.add(cursor);
        if (pattern != null) {
            args.add("MATCH");
            args.add(pattern);
        }
        if (count != null) {
            args.add("COUNT");
            args.add(count);
        }

        if (type != null) {
            args.add("TYPE");
            args.add(type);
        }
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.scan(args)
        );
    }
}
