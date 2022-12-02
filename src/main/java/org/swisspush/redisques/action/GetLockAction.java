package org.swisspush.redisques.action;


import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.swisspush.redisques.handler.GetLockHandler;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUENAME;

public class GetLockAction implements QueueAction {

    private RedisAPI redisAPI;
    private String locksKey;

    private Logger log;

    public GetLockAction(RedisAPI redisAPI, String locksKey, Logger log) {
        this.redisAPI = redisAPI;
        this.locksKey = locksKey;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        final JsonObject body = event.body();
        if (null == body) {
            log.warn("Got msg with empty body from event bus. We'll run directly in " +
                    "a NullPointerException now. address={}  replyAddress={} ", event.address(), event.replyAddress());
            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
        }
        redisAPI.hget(locksKey, body.getJsonObject(PAYLOAD).getString(QUEUENAME), new GetLockHandler(event));
    }

}
