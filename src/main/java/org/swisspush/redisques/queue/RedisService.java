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
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.script(Arrays.asList(name, sha)).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> evalsha(List<String> args) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.evalsha(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> info(List<String> args) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.info(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> lpop(List<String> args) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lpop(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> exists(List<String> args) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.exists(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> del(List<String> args) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.del(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hmset(List<String> args) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hmset(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> get(String key) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.get(key).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Boolean> setNxPx(String key, String value, boolean nx, long duration) {
        JsonArray options = new JsonArray();
        options.add("PX").add(duration);
        if (nx) {
            options.add("NX");
        }
        Promise<Boolean> p = Promise.promise();
        redis().compose((RedisAPI redisAPI) ->
                redisAPI.send(Command.SET, RedisUtils.toPayload(key, value, options).toArray(new String[0]))
                        .onSuccess(resp -> p.complete(resp != null && "OK".equals(resp.toString())))
                        .onFailure(p::fail)
        );
        return p.future();
    }

    public Future<Boolean> setNxEx(String key, String value, boolean nx, long duration) {
        JsonArray opts = new JsonArray().add("EX").add(duration);
        if (nx) {
            opts.add("NX");
        }
        Promise<Boolean> p = Promise.promise();
        redis().compose((RedisAPI redisAPI) ->
                redisAPI.send(Command.SET, RedisUtils.toPayload(key, value, opts).toArray(new String[0]))
                        .onSuccess(resp -> p.complete(resp != null && "OK".equals(resp.toString())))
                        .onFailure(p::fail)
        );
        return p.future();
    }

    public Future<Response> zadd(String queuesKey, String queueName, String value) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zadd(Arrays.asList(queuesKey, value, queueName)).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hset(String queuesKey, String queueName, String value) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hset(Arrays.asList(queuesKey, queueName, value)).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hdel(String key, String queueName) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hdel(Arrays.asList(key, queueName)).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hdels(List<String> args) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hdel(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> llen(String key) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.llen(key).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hvals(String key) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hvals(key).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hget(String key, String field) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hget(key, field).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }
    public Future<Response> ltrim(String key, String start, String stop) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.ltrim(key, start, stop).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> lrange(String key, String start, String stop) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lrange(key, start, stop).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hexists(String key, String value) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hexists(key, value).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> lindex(String key, String index) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lindex(key, index).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> lset(String key, String index, String element) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lset(key, index, element).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> lrem(String key, String count, String element) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lrem(key, count, element).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> zremrangebyscore(String key, String min, String max) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zremrangebyscore(key, min, max).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> zrangebyscore(String key, String min, String max) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zrangebyscore(Arrays.asList(key, min, max)).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> zcount(String key, String min, String max) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zcount(key, min, max).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> rpush(String key, String item) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.rpush(Arrays.asList(key, item)).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> hkeys(String key) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hkeys(key).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> keys(String pattern) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.keys(pattern).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> expire(String key, String seconds) {
        Promise<Response> p = Promise.promise();
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.expire(Arrays.asList(key, seconds)).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }

    public Future<Response> scan(String cursor, @Nullable String pattern, @Nullable String count, @Nullable String type) {
        Promise<Response> p = Promise.promise();
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
                redisAPI.scan(args).onSuccess(resp -> p.complete())
                        .onFailure(p::fail)
        );
    }
}
