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

/**
 * Service wrapper around {@link RedisAPI} providing convenience methods
 * for commonly used Redis commands in RedisQues.
 *
 * <p>
 * All methods return Vert.x {@link Future}s and are non-blocking.
 * </p>
 */
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

    /**
     * Redis script command.
     *
     * @param name  Lua script command
     * @param value value pass in
     * @return a {@link Future} containing the SHA1 hash of the loaded script
     */
    public Future<Response> script(String name, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.script(Arrays.asList(name, value))
        );
    }

    /**
     * Executes a previously loaded Lua script using its SHA1 hash.
     *
     * @param args arguments
     * @return a {@link Future} containing the script execution result
     */
    public Future<Response> evalsha(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.evalsha(args)
        );
    }

    /**
     * Retrieves Redis server information.
     *
     * @return a {@link Future} containing Redis INFO response
     */
    public Future<Response> info(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.info(args)
        );
    }

    /**
     * Removes and returns the first element of a list.
     *
     * @param args Redis list key
     * @return a {@link Future} containing the popped value, or null if empty
     */
    public Future<Response> lpop(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lpop(args)
        );
    }

    /**
     * Checks whether a key exists.
     *
     * @param args Redis key
     * @return a {@link Future} containing 1 if the key exists, otherwise 0
     */
    public Future<Response> exists(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.exists(args)
        );
    }

    /**
     * Deletes a key from Redis.
     *
     * @param args Redis key
     * @return a {@link Future} containing the number of deleted keys
     */
    public Future<Response> del(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.del(args)
        );
    }

    /**
     * Sets multiple hash fields at once.
     *
     * @param args Redis hash key, and field-value pairs
     * @return a {@link Future} containing Redis response
     */
    public Future<Response> hmset(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hmset(args)
        );
    }

    /**
     * Gets the value of a key.
     *
     * @param key Redis key
     * @return a {@link Future} containing the stored value or null
     */
    public Future<Response> get(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.get(key)
        );
    }


    /**
     * Sets a key only if it does not already exist, with a TTL.
     *
     * @param key            Redis key
     * @param value          value to store
     * @param setNx          add NX flag
     * @param durationMillis expiration duration
     * @return a {@link Future} indicating whether the key was set
     */
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

    /**
     * Adds or updates a member in a sorted set.
     *
     * @param key   sorted set key
     * @param score member score
     * @param value member value
     * @return a {@link Future} containing the number of added elements
     */
    public Future<Response> zadd(String key, String value, String score) {
        return zadd(key, List.of(), value, score);
    }

    /**
     * Adds or updates a member in a sorted set.
     *
     * @param key    sorted set key
     * @param params addtional flag list
     * @param score  member score
     * @param value  member value
     * @return a {@link Future} containing the number of added elements
     */
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

    /**
     * Retrieves the score of a member in a sorted set.
     *
     * @param key   the key of the sorted set
     * @param value the member whose score is to be retrieved
     * @return a {@link Future} that completes with the Redis {@link Response}
     * containing the score of the member, or {@code null} if the member
     * does not exist in the sorted set
     */
    public Future<Response> zscore(String key, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zscore(key, value)
        );
    }

    /**
     * Removes a member from a sorted set.
     *
     * @param key   the key of the sorted set
     * @param value the member to be removed from the sorted set
     * @return a {@link Future} that completes with the Redis {@link Response}
     * indicating the number of removed elements
     */
    public Future<Response> zrem(String key, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zrem(Arrays.asList(key, value))
        );
    }

    /**
     * Sets the value of a field in a hash.
     *
     * @param queuesKey the key of the hash
     * @param queueName the field name within the hash
     * @param value     the value to set for the specified field
     * @return a {@link Future} that completes with the Redis {@link Response}
     * indicating whether the field was newly added or updated
     */
    public Future<Response> hset(String queuesKey, String queueName, String value) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hset(Arrays.asList(queuesKey, queueName, value))
        );
    }

    /**
     * Removes a field from a hash.
     *
     * @param key   Redis hash key
     * @param field field to remove
     * @return a {@link Future} containing number of removed fields
     */
    public Future<Response> hdel(String key, String field) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hdel(Arrays.asList(key, field))
        );
    }

    /**
     * Removes multiple fields from a hash.
     *
     * @param args Redis  keys and fields to remove
     * @return a {@link Future} containing number of removed fields
     */
    public Future<Response> hdel(List<String> args) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hdel(args)
        );
    }

    /**
     * Returns the length of a list.
     *
     * @param key Redis list key
     * @return a {@link Future} containing the list length
     */
    public Future<Response> llen(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.llen(key)
        );
    }

    /**
     * Returns all values of a hash.
     *
     * @param key Redis hash key
     * @return a {@link Future} containing all hash values
     */
    public Future<Response> hvals(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hvals(key)
        );
    }

    /**
     * Retrieves a field value from a hash.
     *
     * @param key   Redis hash key
     * @param field hash field
     * @return a {@link Future} containing the field value or null
     */
    public Future<Response> hget(String key, String field) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hget(key, field)
        );
    }

    /**
     * Trims a list to the specified range.
     *
     * @param key   Redis list key
     * @param start start index
     * @param stop  stop index
     * @return a {@link Future} indicating success
     */
    public Future<Response> ltrim(String key, String start, String stop) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.ltrim(key, start, stop)
        );
    }

    /**
     * Returns a range of elements from a list.
     *
     * @param key   Redis list key
     * @param start start index
     * @param stop  stop index
     * @return a {@link Future} containing the requested elements
     */
    public Future<Response> lrange(String key, String start, String stop) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lrange(key, start, stop)
        );
    }

    /**
     * Checks whether a hash field exists.
     *
     * @param key   Redis hash key
     * @param field hash field
     * @return a {@link Future} containing 1 if exists, otherwise 0
     */
    public Future<Response> hexists(String key, String field) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hexists(key, field)
        );
    }

    /**
     * Returns the element at a specific index in a list.
     *
     * @param key   Redis list key
     * @param index element index
     * @return a {@link Future} containing the element or null
     */
    public Future<Response> lindex(String key, String index) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lindex(key, index)
        );
    }

    /**
     * Sets the value of an element in a list by index.
     *
     * @param key     Redis list key
     * @param index   index to update
     * @param element new value
     * @return a {@link Future} indicating success
     */
    public Future<Response> lset(String key, String index, String element) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lset(key, index, element)
        );
    }

    /**
     * Removes elements equal to a value from a list.
     *
     * @param key     Redis list key
     * @param count   removal count (0 = all)
     * @param element value to remove
     * @return a {@link Future} containing number of removed elements
     */
    public Future<Response> lrem(String key, String count, String element) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.lrem(key, count, element)
        );
    }

    /**
     * Removes sorted set members with scores within a range.
     *
     * @param key Redis sorted set key
     * @param min minimum score
     * @param max maximum score
     * @return a {@link Future} containing number of removed members
     */
    public Future<Response> zremrangebyscore(String key, String min, String max) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zremrangebyscore(key, min, max)
        );
    }

    /**
     * Returns sorted set members with scores within a range.
     *
     * @param key Redis sorted set key
     * @param min minimum score
     * @param max maximum score
     * @return a {@link Future} containing the matching members
     */
    public Future<Response> zrangebyscore(String key, String min, String max) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zrangebyscore(Arrays.asList(key, min, max))
        );
    }

    /**
     * Counts sorted set members within a score range.
     *
     * @param key Redis sorted set key
     * @param min minimum score
     * @param max maximum score
     * @return a {@link Future} containing the count
     */
    public Future<Response> zcount(String key, String min, String max) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.zcount(key, min, max)
        );
    }

    /**
     * Appends one or more values to a list.
     *
     * @param key  Redis list key
     * @param item values to append
     * @return a {@link Future} containing the new list length
     */
    public Future<Response> rpush(String key, String item) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.rpush(Arrays.asList(key, item))
        );
    }


    /**
     * Returns all field names of a hash.
     *
     * @param key Redis hash key
     * @return a {@link Future} containing field names
     */
    public Future<Response> hkeys(String key) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.hkeys(key)
        );
    }

    /**
     * Returns all keys matching a pattern.
     *
     * @param pattern key pattern
     * @return a {@link Future} containing matching keys
     */
    public Future<Response> keys(String pattern) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.keys(pattern)
        );
    }

    /**
     * Sets a timeout on a key.
     *
     * @param key     Redis key
     * @param seconds time to live
     * @return a {@link Future} indicating if the timeout was set
     */
    public Future<Response> expire(String key, String seconds) {
        return redis().compose((RedisAPI redisAPI) ->
                redisAPI.expire(Arrays.asList(key, seconds))
        );
    }


    /**
     * Iterates over keys in the database incrementally.
     *
     * @param cursor  scan cursor
     * @param pattern optional match pattern
     * @param count   optional batch size
     * @param type    optional key type filter
     * @return a {@link Future} containing scan result
     */
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
