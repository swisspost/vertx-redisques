package org.swisspush.redisques.util;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.internal.StringUtil;
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
import java.util.stream.Collectors;

public class QueueConfigurationProvider {
    private static final Logger log = LoggerFactory.getLogger(QueueConfigurationProvider.class);
    public static final String DELETE = "DELETE";

    // Identify of the config provider, should be the same in the same vertx instance, but different in cluster node
    private final String uid = UUID.randomUUID().toString();
    private final String QUEUE_CONFIG_EVENTBUS_SYNC_KEY = "redisques_queue_config_eventbus_sync";
    private final String QUEUE_CONFIG_SENDER_ID = "queue_config_sender_id";
    private final Vertx vertx;
    private final Map<String, QueueConfiguration> queueConfigurations = new ConcurrentHashMap<>();
    public final List<QueueConfiguration> defaultQueueConfigurations;

    private QueueConfigurationProvider(Vertx vertx, List<QueueConfiguration> defaultQueueConfigurations) {
        this.vertx = vertx;
        this.defaultQueueConfigurations = defaultQueueConfigurations;
        loadStaticConfigs();
        vertx.eventBus().consumer(QUEUE_CONFIG_EVENTBUS_SYNC_KEY, (Handler<Message<JsonObject>>) event -> {

            // message structure
            // {
            //   "queue_config_sender_id": "uid",       #The sender's UID, to prevents sender consumer the message from itself
            //   "operation": "DELETE",                 #The operation of the message
            //   "configName": "name for config set",   #The name to identify the config
            //   "payload": {                           #The config itself
            //                "pattern": "queue.filter.regex",
            //                "maxQueueEntries": 0,
            //                "enqueueDelayFactorMillis": 0,
            //                "enqueueMaxDelayMillis": 0,
            //                "retryIntervals": [1, 2, 3]
            //              }
            // }
            if (!event.body().containsKey(QUEUE_CONFIG_SENDER_ID)) {
                return;
            }
            JsonObject body = event.body();
            if (uid.equals(body.getString(QUEUE_CONFIG_SENDER_ID))) {
                log.debug("publish msg from my self, drop it.");
                return;
            }
            if (body.containsKey(RedisquesAPI.OPERATION)) {
                String operation = body.getString(RedisquesAPI.OPERATION);
                if (DELETE.equals(operation)) {
                    final String configName = event.body().getString(RedisquesAPI.PER_QUEUE_CONFIG_NAME);
                    queueConfigurations.remove(configName);
                    log.debug("delete config {} from instance {}", configName, uid);
                } else {
                    log.warn("Unsupported operation: {}", operation);
                }
            } else {
                // we need a message have both name and config body for add or update
                if (body.containsKey(RedisquesAPI.PAYLOAD) && body.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_NAME)) {
                    String name = body.getString(RedisquesAPI.PER_QUEUE_CONFIG_NAME);
                    updateQueueConfigurationInternal(name, body.getJsonObject(RedisquesAPI.PAYLOAD));
                }
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
     * @param name a name use for search the config
     * @return queue configuration if existed, otherwise NULL
     */
    public QueueConfiguration getQueueConfiguration(String name) {
        return queueConfigurations.get(name);
    }

    /**
     * add a new queue configuration, update if already exite, also publish to all other instances
     * Note: the pattern will not be updated if a config already exist
     *
     * @param configName a name of config
     * @param jsonObject json object witch contains the setting value
     */
    public void updateQueueConfiguration(String configName, JsonObject jsonObject) {

        updateQueueConfigurationInternal(configName, jsonObject);
        // publish to other instances in the cluster.
        JsonObject payload = new JsonObject();
        payload.put(QUEUE_CONFIG_SENDER_ID, uid);
        payload.put(RedisquesAPI.PAYLOAD, jsonObject);
        payload.put(RedisquesAPI.PER_QUEUE_CONFIG_NAME, configName);
        vertx.eventBus().publish(QUEUE_CONFIG_EVENTBUS_SYNC_KEY, payload);
    }

    /**
     * remove a config from current instance, also publish to all other instances
     * @param configName the config name will delete
     */
    public void removeQueueConfiguration(String configName) {
        queueConfigurations.remove(configName);
        // publish to other instances in the cluster.
        JsonObject payload = new JsonObject();
        payload.put(QUEUE_CONFIG_SENDER_ID, uid);
        payload.put(RedisquesAPI.OPERATION, DELETE);
        payload.put(RedisquesAPI.PER_QUEUE_CONFIG_NAME, configName);
        vertx.eventBus().publish(QUEUE_CONFIG_EVENTBUS_SYNC_KEY, payload);
    }

    /**
     * get one or all configurations, if a name is passed in, the matched one will return, if exists.
     * if "*" passed in all will return.
     * @param name a config name or a "*" to match all.
     * @return
     */
    public Map<String, QueueConfiguration> getQueueConfigurations(String name) {
        if (name.equals("*")) {
            return queueConfigurations;
        }
        return queueConfigurations.entrySet()
                .stream()
                .filter(e -> name.equals(e.getKey()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    private void updateQueueConfigurationInternal(String name, JsonObject jsonObject) {
        QueueConfiguration queueConfiguration = getQueueConfiguration(name);

        boolean isNew = false;

        final String pattern = jsonObject.getString(RedisquesAPI.PER_QUEUE_CONFIG_PATTERN);

        if (StringUtil.isNullOrEmpty(pattern)) {
            throw new IllegalArgumentException("queue configuration pattern is empty");
        }

        if (queueConfiguration == null) {
            // we don't have a config with given name.
            queueConfiguration = new QueueConfiguration(pattern);
            isNew = true;
        }

        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_NUMBER_OF_ITEM_BATCH_DISPATCH)) {
            queueConfiguration.withNumberOfBatchItemDispatch(jsonObject.getInteger(RedisquesAPI.PER_QUEUE_CONFIG_NUMBER_OF_ITEM_BATCH_DISPATCH));
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_ENTRIES)) {
            queueConfiguration.withMaxQueueEntries(jsonObject.getInteger(RedisquesAPI.PER_QUEUE_CONFIG_MAX_QUEUE_ENTRIES));
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS)) {
            queueConfiguration.withEnqueueDelayMillisPerSize(jsonObject.getInteger(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_DELAY_FACTOR_MILLIS));
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS)) {
            queueConfiguration.withEnqueueMaxDelayMillis(jsonObject.getInteger(RedisquesAPI.PER_QUEUE_CONFIG_ENQUEUE_MAX_DELAY_MILLIS));
        }
        if (jsonObject.containsKey(RedisquesAPI.PER_QUEUE_CONFIG_RETRY_INTERVALS)) {
            queueConfiguration.withRetryIntervals(jsonObject.getJsonArray(RedisquesAPI.PER_QUEUE_CONFIG_RETRY_INTERVALS).stream()
                    .mapToInt(v -> ((Number) v).intValue())
                    .toArray());
        }

        // exists one is updates by reference
        if (isNew) {
            queueConfigurations.put(name, queueConfiguration);
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
