package org.swisspush.redisques.action;

import io.netty.util.internal.StringUtil;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueConfigurationProvider;
import org.swisspush.redisques.util.RedisquesAPI;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;

public class DeletePerQueueConfigurationsAction implements QueueAction {
    private final Logger log;
    private final QueueConfigurationProvider redisquesConfigurationProvider;

    public DeletePerQueueConfigurationsAction(QueueConfigurationProvider queueConfigurationProvider, Logger log) {
        this.redisquesConfigurationProvider = queueConfigurationProvider;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject payload = event.body().getJsonObject(PAYLOAD);
        String configName = payload.getString(RedisquesAPI.PER_QUEUE_CONFIG_NAME);
        log.debug("will remove config config {} from provider", configName);
        if (StringUtil.isNullOrEmpty(configName)) {
            event.reply(createErrorReply());
            return;
        }
        redisquesConfigurationProvider.removeQueueConfiguration(configName);
        event.reply(createOkReply());
    }
}
