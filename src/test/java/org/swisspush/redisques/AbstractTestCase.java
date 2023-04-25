package org.swisspush.redisques;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.runner.RunWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.swisspush.redisques.util.RedisquesAPI.REQUESTED_BY;

@RunWith(VertxUnitRunner.class)
public abstract class AbstractTestCase {

    protected static final String TIMESTAMP = "timestamp";
    protected static final String PROCESSOR_ADDRESS = "processor-address";

    protected static Logger log = LoggerFactory.getLogger(AbstractTestCase.class);

    protected static Vertx vertx;
    protected static Jedis jedis;

    protected static String deploymentId = "";

    protected void flushAll(){
        if(jedis != null){
            jedis.flushAll();
        }
    }

    protected String getRedisquesAddress() {return "redisques"; }

    protected String getRedisPrefix() {return "redisques:"; }

    protected String getLocksRedisKey(){
        return getRedisPrefix() + "locks";
    }

    protected String getQueuesRedisKeyPrefix(){ return getRedisPrefix() + "queues:"; }

    protected String getConsumersRedisKeyPrefix(){ return  getRedisPrefix() + "consumers:"; }

    protected void assertKeyCount(TestContext context, int keyCount){
        assertKeyCount(context, "", keyCount);
    }

    protected void assertKeyCount(TestContext context, String prefix, int keyCount){
        context.assertEquals(keyCount, jedis.keys(prefix+"*").size());
    }

    protected void assertLockContent(TestContext context, String queuename, String expectedRequestedByValue){
        String item = jedis.hget(getLocksRedisKey(), queuename);
        context.assertNotNull(item);
        if(item != null){
            JsonObject lockInfo = new JsonObject(item);
            context.assertNotNull(lockInfo.getString(REQUESTED_BY), "Property '"+REQUESTED_BY+"' missing");
            context.assertNotNull(lockInfo.getLong(TIMESTAMP), "Property '"+TIMESTAMP+"' missing");
            context.assertEquals(expectedRequestedByValue, lockInfo.getString(REQUESTED_BY), "Property '"+REQUESTED_BY+"' has wrong value");
        }
    }

    protected void delay(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(iex);
        }
    }

    @AfterClass
    public static void stopRedis(TestContext context) {
        jedis.close();
    }

    @Before
    public void cleanDB() {
        flushAll();
    }

    protected void eventBusSend(JsonObject operation, Handler<AsyncResult<Message<JsonObject>>> handler){
        vertx.eventBus().request(getRedisquesAddress(), operation, handler);
    }

    protected void assertQueueItemsCount(TestContext context, String queue, int count){
        context.assertEquals((long)count, jedis.llen(getQueuesRedisKeyPrefix() + queue));
    }

    protected void assertQueuesCount(TestContext context, int count){
        assertKeyCount(context, getQueuesRedisKeyPrefix(), count);
    }

    protected void assertLocksCount(TestContext context, int count){
        context.assertEquals((long)count, jedis.hlen(getLocksRedisKey()));
    }

    protected void assertLockExists(TestContext context, String lock){
        context.assertTrue(jedis.hexists(getLocksRedisKey(), lock), "expected lock '"+lock+"' to exist");
    }

    protected void assertLockDoesNotExist(TestContext context, String lock){
        context.assertFalse(jedis.hexists(getLocksRedisKey(), lock), "expected lock '"+lock+"' to not exist");
    }

    protected void lockQueue(String queue){
        JsonObject lockInfo = new JsonObject();
        lockInfo.put(REQUESTED_BY, "unit_test");
        lockInfo.put("timestamp", System.currentTimeMillis());
        Map<String,String> values = new HashMap<>();
        values.putIfAbsent(queue, lockInfo.encode());
        jedis.hmset(getLocksRedisKey(), values);
    }
}