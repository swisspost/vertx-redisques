package org.swisspush.redisques.action;

import io.netty.util.internal.StringUtil;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;

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
        if (configurationValues == null) {
            log.info("Configuration is null or empty.");
            event.reply(createOkReply());
            return;
        }

        String configName = configurationValues.getString(RedisquesAPI.PER_QUEUE_CONFIG_NAME);
        String configPattern = configurationValues.getString(RedisquesAPI.PER_QUEUE_CONFIG_PATTERN);
        String filterPattern = configurationValues.getString(RedisquesAPI.FILTER);

        // if we have a pattern from config itself, use it. ignore the one from FILTER
        filterPattern = StringUtil.isNullOrEmpty(configPattern) ? filterPattern : configPattern;

        if (StringUtil.isNullOrEmpty(filterPattern)) {
            event.reply(createErrorReply().put(MESSAGE, "Pattern is required"));
            return;
        }

        //The config created from code, doesn't have a name, so use the pattern as name
        if (StringUtil.isNullOrEmpty(configName)) {
            configName = filterPattern;
        }
        configurationValues.put(RedisquesAPI.PER_QUEUE_CONFIG_PATTERN, filterPattern);
        redisquesConfigurationProvider.updateQueueConfiguration(configName, configurationValues);
        event.reply(createOkReply());
    }
}
