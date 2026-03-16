package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;

import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

public class GetConfigurationAction implements QueueAction {

    private final RedisquesConfigurationProvider redisquesConfigurationProvider;

    public GetConfigurationAction(RedisquesConfigurationProvider redisquesConfigurationProvider) {
        this.redisquesConfigurationProvider = redisquesConfigurationProvider;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        event.reply(createOkReply().put(VALUE, redisquesConfigurationProvider.configuration().asJsonObject()));
    }
}
