package org.swisspush.redisques.queue;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.ZModem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.util.RedisClusterUtil;
import org.swisspush.redisques.util.RedisProvider;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service wrapper around {@link RedisAPI} providing convenience methods
 * for commonly used Redis commands in RedisQues.
 *
 * <p>
 * All methods return Vert.x {@link Future}s and are non-blocking.
 * </p>
 */
public class RedisService {
    private static final Logger log = LoggerFactory.getLogger(RedisService.class);
    private final RedisProvider redisProvider;
    private final Vertx vertx;
    private final RedisquesConfigurationProvider configurationProvider;
    public static volatile AtomicBoolean isClusterMode = new AtomicBoolean(false);
    public static final int MAX_COMMANDS_IN_BATCH = 100;

    private static class RedisNode {
        String host;
        int port;

        RedisNode(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    public RedisService(Vertx vertx, RedisProvider redisProvider, RedisquesConfigurationProvider configurationProvider) {
        this.redisProvider = redisProvider;
        this.vertx = vertx;
        this.configurationProvider = configurationProvider;
    }

    private Future<RedisAPI> redis() {
        return redisProvider.redis().onFailure(
                throwable -> log.warn("Redis: Failed to get RedisAPI", throwable)
        ).compose(Future::succeededFuture);
    }

    /**
     * execute a command
     * @param request redi commands will execute
     * @return a list of responses
     */
    public Future<Response> send(Request request) {
        return redisProvider.redisConnection().compose((RedisConnection connection) ->
                connection.send(request)
        );
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
     * Get the values of keys.
     *
     * @param keys keys to fetch
     * @return a {@link Future} containing the stored values or null
     */
    public Future<List<Response>> get(List<String> keys) {
        if (keys.size() > MAX_COMMANDS_IN_BATCH) {
            throw new IllegalArgumentException("get exceeds max keys in batch");
        }
        return redisProvider.redisConnection().compose(connection -> {
            List<Request> requests = new ArrayList<>();

            for (String key : keys) {
                requests.add(Request.cmd(Command.GET).arg(key));
            }
            return this.batch(requests);
        });
    }

    /**
     * Sets a key with value
     *
     * @param key            Redis key
     * @param value          value to store
     * @return a {@link Future} indicating whether the key was set
     */
    public Future<Boolean> set(String key, String value) {
        return redis().compose((RedisAPI redisAPI) -> redisAPI.send(Command.SET, key, value)).map((Response rsp) -> rsp != null && "OK".equals(rsp.toString()));
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
        }).map((Response rsp) -> rsp != null && "OK".equals(rsp.toString()));
    }

    /**
     * update TTL of a key only if existed.
     *
     * @param key            Redis key
     * @param durationMillis expiration duration
     * @return a {@link Future} indicating whether the key was set
     */
    public Future<Boolean> setPExpire(String key, long durationMillis) {
        return redis().compose((RedisAPI redisAPI) -> {
            String durationMillisStr = String.valueOf(durationMillis);
            return redisAPI.send(Command.PEXPIRE, key, durationMillisStr);
        }).map((Response rsp) -> rsp != null && rsp.toInteger() == 1);
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

    /**
     * get mutiple values of keys
     *
     * @param keys
     * @return a list of responses
     */
    public Future<Response> mget(List<String> keys) {
        if (keys.size() > MAX_COMMANDS_IN_BATCH) {
            throw new IllegalArgumentException("mget exceeds max keys in batch");
        }
        Request req = Request.cmd(Command.MGET);
        for (String key : keys) {
            req.arg(key);
        }
        return redisProvider.redisConnection().compose((RedisConnection connection) ->
                connection.send(req)
        );
    }

    /**
     * execute a list of command in pipeline
     *
     * @param batchCmd redis commands will execute
     * @return a list of responses
     */
    public Future<List<Response>> batch(List<Request> batchCmd) {
        if (batchCmd.size() > MAX_COMMANDS_IN_BATCH) {
            throw new IllegalArgumentException("batchCmd exceeds max commands in batch");
        }
        return redisProvider.redisConnection().compose((RedisConnection connection) ->
                connection.batch(batchCmd)
        );
    }


    /**
     * Cluster wide scanner, if cluster enabled, if not a cluster, will run scan directly
     **/
    public Future<Map<String, String>> scanCluster(String pattern) {
        Promise<Map<String, String>> promise = Promise.promise();
        Map<String, String> allKeysWithValue = new ConcurrentHashMap<>();
        if (configurationProvider.configuration().getRedisClientType() == RedisClientType.CLUSTER) {
            // 1. Get cluster slots info
            send(Request.cmd(Command.CLUSTER).arg("SLOTS")).onComplete(slotsRes -> {
                if (slotsRes.failed()) {
                    promise.fail(slotsRes.cause());
                    return;
                }
                //Extract all master node on cluster
                List<RedisNode> masterNodes = new ArrayList<>();
                for (Response slotInfo : slotsRes.result()) {
                    /**
                     * [0: start_slot, 1: end_slot,
                     *   2: [0: master_ip, 1: master_port, master_id],
                     *   [replica1_ip, replica1_port, replica1_id],
                     *   [replica2_ip, replica2_port, replica2_id],
                     */
                    Response master = slotInfo.get(2); // index 2 = master node info
                    String host = master.get(0).toString();
                    int port = master.get(1).toInteger();
                    masterNodes.add(new RedisNode(host, port));
                }

                List<Future<?>> futures = new ArrayList<>();

                // 2. Scan each master node
                for (RedisNode node : masterNodes) {
                    futures.add(scanSingleNode(node, pattern, allKeysWithValue));
                }
                Future.all(futures).onComplete(done -> {
                    if (done.failed()) {
                        promise.fail(done.cause());
                    } else {
                        promise.complete(allKeysWithValue);
                    }
                });
            });
        } else {
            Promise<Void> singleScanPromise = Promise.promise();
            redisProvider.redisConnection().onComplete(connection -> {
                scanRecursive(connection.result(), "0", pattern, allKeysWithValue, singleScanPromise);
                singleScanPromise.future().onComplete(done -> {
                    if (done.failed()) {
                        promise.fail(done.cause());
                    } else {
                        promise.complete(allKeysWithValue);
                    }
                });
            });
        }

        return promise.future();
    }

    /**
     * create a standalone connection config for a redis master node
     *
     * @param node
     * @return
     */
    private RedisOptions createDirectConnectionRedisOpt(RedisNode node) {
        RedisquesConfiguration config = configurationProvider.configuration();
        String redisAuth = config.getRedisAuth();
        RedisOptions directConnectionRedisOptions = new RedisOptions()
                .setPassword((redisAuth == null ? "" : redisAuth))
                .setType(RedisClientType.STANDALONE); // use a standalone connectio
        if (config.getRedisEnableTls()) {
            NetClientOptions netClientOptions = directConnectionRedisOptions.getNetClientOptions();
            netClientOptions.setSsl(true)
                    .setHostnameVerificationAlgorithm("HTTPS");
            directConnectionRedisOptions.setNetClientOptions(netClientOptions);
        }
        directConnectionRedisOptions.addConnectionString(createConnectString(node));
        return directConnectionRedisOptions;
    }

    private String createConnectString(RedisNode node) {
        RedisquesConfiguration config = configurationProvider.configuration();
        String redisPassword = config.getRedisPassword();
        String redisUser = config.getRedisUser();
        StringBuilder connectionStringPrefixBuilder = new StringBuilder();
        // protocol
        connectionStringPrefixBuilder.append(config.getRedisEnableTls() ? "rediss://" : "redis://");
        // auth
        if (redisUser != null && !redisUser.isEmpty()) {
            connectionStringPrefixBuilder.append(redisUser).append(":")
                    .append(redisPassword == null ? "" : redisPassword)
                    .append("@");
        }
        connectionStringPrefixBuilder.append(node.host).append(":").append(node.port);
        return connectionStringPrefixBuilder.toString();
    }

    /**
     * connect to a redis node directly
     *
     * @param node
     * @param pattern
     * @param collector
     * @return
     */
    private Future<Void> scanSingleNode(RedisNode node,
                                        String pattern, Map<String, String> collector) {
        Promise<Void> promise = Promise.promise();
        Redis.createClient(vertx, createDirectConnectionRedisOpt(node)).connect(ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                return;
            }
            RedisConnection conn = ar.result();
            scanRecursive(conn, "0", pattern, collector, promise);
        });

        return promise.future();
    }

    /**
     * scan a node with given pattern
     *
     * @param conn
     * @param cursor
     * @param pattern
     * @param collector
     * @param promise
     */
    private void scanRecursive(RedisConnection conn,
                               String cursor,
                               String pattern,
                               Map<String, String> collector,
                               Promise<Void> promise) {

        conn.send(Request.cmd(Command.SCAN)
                        .arg(cursor)
                        .arg("MATCH").arg(pattern)
                        .arg("COUNT").arg(MAX_COMMANDS_IN_BATCH),
                ar -> {
                    if (ar.failed()) {
                        promise.fail(ar.cause());
                        return;
                    }
                    Response res = ar.result();
                    String nextCursor = res.get(0).toString();
                    List<Response> keys = res.get(1).stream().collect(Collectors.toList());
                    if (keys.isEmpty()) {
                        // continue scanning
                        if ("0".equals(nextCursor)) {
                            promise.complete();
                        } else {
                            scanRecursive(conn, nextCursor, pattern, collector, promise);
                        }
                        return;
                    }
                    // Fetch values (cluster-safe: GET per key batch in slot group)
                    // Group keys by slot
                    Map<Integer, List<String>> slotGroups = new HashMap<>();
                    for (Response key : keys) {
                        String k = key.toString();
                        int slot = ZModem.generate(k); // CRC16 slot
                        slotGroups.computeIfAbsent(slot, s -> new ArrayList<>()).add(k);
                    }

                    // get all keys in group
                    List<Future<Map<String, String>>> futures = new ArrayList<>();
                    for (List<String> groupKeys : slotGroups.values()) {

                        List<Request> batch = new ArrayList<>();
                        for (String k : groupKeys) {
                            batch.add(Request.cmd(Command.GET).arg(k));
                        }
                        Future<Map<String, String>> f = batch(batch)
                                .map(responses -> {
                                    Map<String, String> result = new HashMap<>();
                                    for (int i = 0; i < groupKeys.size(); i++) {
                                        Response r = responses.get(i);
                                        result.put(groupKeys.get(i), r == null ? null : r.toString());
                                    }
                                    return result;
                                });

                        futures.add(f);
                    }

                    Future.all(new ArrayList<>(futures))
                            .map(cf -> {
                                Map<String, String> finalResult = new HashMap<>();
                                // merge all response
                                for (int i = 0; i < futures.size(); i++) {
                                    collector.putAll(futures.get(i).result());
                                }
                                return finalResult;
                            }).onComplete(event -> {
                                if (event.failed()) {
                                    log.error("get values from node failed");
                                    promise.fail(event.cause());
                                    return;
                                }
                                // continue recursion
                                if ("0".equals(nextCursor)) {
                                    promise.complete();
                                } else {
                                    scanRecursive(conn, nextCursor, pattern, collector, promise);
                                }
                            });
                });
    }

    /**
     * cluster safe execute a list of command in pipeline, when in cluster mode
     * the batch will split into slots by keys
     * @param cmd redis command will execute as batch
     * @param keys keys will execute
     * @param additionalArgs additional arguments will add to command
     * @return a list of responses
     */
    public Future<List<Response>> clusterSafeBatch(Command cmd, List<String> keys, List<String> additionalArgs) {

        if (keys == null || keys.isEmpty()) {
            return Future.succeededFuture(List.of());
        }
        if (keys.size() > MAX_COMMANDS_IN_BATCH) {
            throw new IllegalArgumentException("batchCmd exceeds max commands in batch");
        }

        Map<Integer, List<Request>> requestsGroupedBySlots = new HashMap<>();

        //Track output ordering, where its responses should go in the final merged result list.
        Map<Integer, List<Integer>> requestIndexes = new HashMap<>();

        if (isClusterMode.get()) {
            // Build requests and group by slot
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                int slot = RedisClusterUtil.calcRedisSlot(key);
                Request req = Request.cmd(cmd).arg(key);
                for(String arg : additionalArgs) {
                    req = req.arg(arg);
                }
                requestsGroupedBySlots.computeIfAbsent(slot, s -> new ArrayList<>()).add(req);
                requestIndexes.computeIfAbsent(slot, s -> new ArrayList<>()).add(i);
            }
        } else {
            // non-cluster single slot
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                Request req = Request.cmd(cmd).arg(key);
                for(String arg : additionalArgs) {
                    req = req.arg(arg);
                }
                requestsGroupedBySlots.computeIfAbsent(1, s -> new ArrayList<>()).add(req);
                requestIndexes.computeIfAbsent(1, s -> new ArrayList<>()).add(i);
            }
        }

        List<Integer> slots = new ArrayList<>(requestsGroupedBySlots.keySet());
        List<Future<?>> futures = new ArrayList<>();

        for (Integer slot : slots) {
            futures.add(this.batch(requestsGroupedBySlots.get(slot)));
        }
        return Future.all(futures)
                .map(cf -> {
                    // Create a list large enough to hold all responses.
                    List<Response> finalResponses =
                            new ArrayList<>(Collections.nCopies(keys.size(), null));
                    // merge response from each slot
                    for (int f = 0; f < slots.size(); f++) {
                        Integer slot = slots.get(f);
                        List<Integer> indexes = requestIndexes.get(slot);
                        List<Response> responses = cf.resultAt(f);
                        for (int i = 0; i < indexes.size(); i++) {
                            finalResponses.set(indexes.get(i), responses.get(i));
                        }
                    }
                    return finalResponses;
                });
    }
}
