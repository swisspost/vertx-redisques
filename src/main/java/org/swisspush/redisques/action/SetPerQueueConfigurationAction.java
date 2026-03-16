package org.swisspush.redisques.action;

import io.netty.util.internal.StringUtil;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfigurationProvider;

import static org.swisspush.redisques.util.RedisquesAPI.FILTER;
import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;
import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;

public class SetPerQueueConfigurationAction implements QueueAction {
    private final Logger log;
    private final QueueConfigurationProvider redisquesConfigurationProvider;

    public SetPerQueueConfigurationAction(QueueConfigurationProvider queueConfigurationProvider, Logger log) {
        this.redisquesConfigurationProvider = queueConfigurationProvider;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        final JsonObject configurationValues = event.body().getJsonObject(PAYLOAD);
        final String pattern = configurationValues.getString(FILTER);
        if (StringUtil.isNullOrEmpty(pattern)) {
            event.reply(createErrorReply().put(MESSAGE, "Pattern is required"));
            return;
        }
        if (configurationValues == null) {
            log.info("Configuration is null or empty.");
            event.reply(createOkReply());
            return;
        }
        redisquesConfigurationProvider.updateQueueConfiguration(pattern, configurationValues);
        event.reply(createOkReply());
    }
}
