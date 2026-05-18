package org.swisspush.redisques.action;

import io.netty.util.internal.StringUtil;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;

import java.util.Map;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;

public class GetPerQueueConfigurationsAction implements QueueAction {
    private final Logger log;
    private final QueueConfigurationProvider redisquesConfigurationProvider;

    public GetPerQueueConfigurationsAction(QueueConfigurationProvider queueConfigurationProvider, Logger log) {
        this.redisquesConfigurationProvider = queueConfigurationProvider;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject payload = event.body().getJsonObject(PAYLOAD);
        String configName = payload.getString(RedisquesAPI.PER_QUEUE_CONFIG_NAME);
        if (StringUtil.isNullOrEmpty(configName)) {
            configName = "*";
        }
        log.debug("get config with name {}", configName);
        Map<String, QueueConfiguration> queueConfigs = redisquesConfigurationProvider.getQueueConfigurations(configName);
        if (queueConfigs == null || queueConfigs.isEmpty()) {
            log.info("Configurations is null or empty.");
            event.reply(new JsonObject());
            return;
        }
        event.reply(JsonObject.mapFrom(queueConfigs));
    }
}
