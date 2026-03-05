package org.swisspush.redisques.util;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class QueueConfigurationProvider {
    private static final Logger log = LoggerFactory.getLogger(QueueConfigurationProvider.class);

    // Identify of the config provider, should be the same in the same vertx instance, but different in cluster node
    private final String uid = UUID.randomUUID().toString();
    private final String QUEUE_CONFIG_EVENTBUS_SYNC_KEY = "redisques_queue_config_eventbus_sync";
    private final String QUEUE_CONFIG_SENDER_ID =  "queue_config_sender_id";
    private final Vertx vertx;
    private final Map<String, QueueConfiguration> queueConfigurations = new ConcurrentHashMap<>();
    public final List<QueueConfiguration> defaultQueueConfigurations;

    private QueueConfigurationProvider(Vertx vertx, List<QueueConfiguration> defaultQueueConfigurations) {
        this.vertx = vertx;
        this.defaultQueueConfigurations = defaultQueueConfigurations;
        loadStaticConfigs();
        vertx.eventBus().consumer(QUEUE_CONFIG_EVENTBUS_SYNC_KEY, (Handler<Message<JsonObject>>) event -> {
            if(!event.body().containsKey(QUEUE_CONFIG_SENDER_ID)){
                return;
            }
            JsonObject body = event.body();
            if(uid.equals(body.getString(QUEUE_CONFIG_SENDER_ID))) {
                log.debug("publish msg from my self, drop it.");
                return;
            }
            if (body.containsKey(RedisquesAPI.PAYLOAD) && body.containsKey(RedisquesAPI.FILTER)) {
                updateQueueConfigurationInternal(body.getString(RedisquesAPI.FILTER), body.getJsonObject(RedisquesAPI.PAYLOAD));
            }
        });
    }

    public static NodeLocalSingletonProvider<QueueConfigurationProvider> provider(Vertx vertx, List<QueueConfiguration> defaultQueueConfigurations) {
        return new NodeLocalSingletonProvider<>(
                vertx,
                "per-queue-config",
                () -> Future.succeededFuture(new QueueConfigurationProvider(vertx, defaultQueueConfigurations)));
    }

    /**
     * return a cluster node based UUID which assigned to the QueueConfiguration
     *
     * @return UUID
     */
    public String getUid() {
        return uid;
    }

    /**
     * find first matching Queue-Configuration
     *
     * @param queueName search first configuration for that queue-name
     * @return null when no queueConfiguration's RegEx matches given queueName - else the QueueConfiguration
     */
    public QueueConfiguration findQueueConfiguration(String queueName) {
        for (QueueConfiguration queueConfiguration : queueConfigurations.values()) {
            if (queueConfiguration.compiledPattern().matcher(queueName).matches()) {
                return queueConfiguration;
            }
        }
        return null;
    }

    /**
     * return an exist queue configuration by filter pattern
     *
     * @param pattern a string of pattern use for search
     * @return queue configuration if existed, otherwise NULL
     */
    public QueueConfiguration getQueueConfiguration(String pattern) {
        return queueConfigurations.get(pattern);
    }

    /**
     * add a new queue configuration, update if already exite
     * @param pattern a string of pattern use for search
     * @param jsonObject json object witch contains the setting value
     */
    public void updateQueueConfiguration(String pattern, JsonObject jsonObject) {

        updateQueueConfigurationInternal(pattern, jsonObject);
        // publish to other instances in the cluster.
        JsonObject payload = new JsonObject();
        payload.put(QUEUE_CONFIG_SENDER_ID, uid);
        payload.put(RedisquesAPI.PAYLOAD, jsonObject);
        payload.put(RedisquesAPI.FILTER, pattern);
        vertx.eventBus().publish(QUEUE_CONFIG_SENDER_ID, payload);
    }

    private void updateQueueConfigurationInternal(String pattern, JsonObject jsonObject) {
        QueueConfiguration queueConfiguration = getQueueConfiguration(pattern);

        boolean isNew = false;
        if  (queueConfiguration == null) {
            queueConfiguration = new QueueConfiguration().withPattern(pattern);
            isNew = true;
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_ENTRIES))
        {
            queueConfiguration.withMaxQueueEntries(jsonObject.getInteger(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_ENTRIES));
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS))
        {
            queueConfiguration.withEnqueueDelayMillisPerSize(jsonObject.getInteger(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS));
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS))
        {
            queueConfiguration.withEnqueueMaxDelayMillis(jsonObject.getInteger(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS));
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_RETRY_INTERVALS))
        {
            queueConfiguration.withRetryIntervals(jsonObject.getJsonArray(RedisquesAPI.PER_QUEUE_CONFIG_RETRY_INTERVALS).stream()
                    .mapToInt(v -> ((Number)v).intValue())
                    .toArray());
        }

        // exists one is updates by reference
        if (isNew) {
            queueConfigurations.put(queueConfiguration.getPattern(), queueConfiguration);
        }
    }

    @VisibleForTesting
    public static void reset() {
       NodeLocalObjectRegistry.reset(QueueConfigurationProvider.class);
    }

    private void loadStaticConfigs() {
        if (defaultQueueConfigurations != null) {
            defaultQueueConfigurations.forEach(queueConfiguration ->
                    queueConfigurations.put(queueConfiguration.getPattern(), queueConfiguration));
        }
    }
}
