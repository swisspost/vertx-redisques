package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;
import org.swisspush.redisques.util.Result;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class SetConfigurationAction implements QueueAction {

    protected final Logger log;
    private final RedisquesConfigurationProvider redisquesConfigurationProvider;

    public SetConfigurationAction(RedisquesConfigurationProvider redisquesConfigurationProvider, Logger log) {
        this.redisquesConfigurationProvider = redisquesConfigurationProvider;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject configurationValues = event.body().getJsonObject(PAYLOAD);
        Result<Void, String> updateResult = redisquesConfigurationProvider.updateConfiguration(configurationValues, true);
        if(updateResult.isOk()) {
            event.reply(createOkReply());
        } else {
            event.reply(createErrorReply().put(MESSAGE, updateResult.getErr()));
        }
    }
}
