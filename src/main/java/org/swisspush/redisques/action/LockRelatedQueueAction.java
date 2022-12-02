package org.swisspush.redisques.action;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;

import java.util.ArrayList;
import java.util.List;

public abstract class LockRelatedQueueAction implements QueueAction {

    protected RedisAPI redisAPI;
    protected String locksKey;

    public LockRelatedQueueAction(RedisAPI redisAPI, String locksKey) {
        this.redisAPI = redisAPI;
        this.locksKey = locksKey;
    }

    protected boolean jsonArrayContainsStringsOnly(JsonArray array) {
        for (Object obj : array) {
            if (!(obj instanceof String)) {
                return false;
            }
        }
        return true;
    }

    protected List<String> buildLocksItems(String locksKey, JsonArray lockNames, JsonObject lockInfo) {
        List<String> list = new ArrayList<>();
        list.add(locksKey);
        String lockInfoStr = lockInfo.encode();
        for (int i = 0; i < lockNames.size(); i++) {
            String lock = lockNames.getString(i);
            list.add(lock);
            list.add(lockInfoStr);
        }
        return list;
    }
}
