package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.RedisquesConfigurationProvider;
import org.swisspush.redisques.util.Result;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class SetConfigurationAction implements QueueAction {

    protected Logger log;
    private RedisquesConfigurationProvider configurationProvider;

    public SetConfigurationAction(RedisquesConfigurationProvider configurationProvider, Logger log) {
        this.configurationProvider = configurationProvider;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject configurationValues = event.body().getJsonObject(PAYLOAD);
        Result<Void, String> updateResult = configurationProvider.updateConfiguration(configurationValues, true);
        if(updateResult.isOk()) {
            event.reply(createOkReply());
        } else {
            event.reply(createErrorReply().put(MESSAGE, updateResult.getErr()));
        }
    }
}
