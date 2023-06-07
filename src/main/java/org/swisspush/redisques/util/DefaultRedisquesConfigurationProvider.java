package org.swisspush.redisques.util;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.swisspush.redisques.util.RedisquesAPI.PROCESSOR_DELAY_MAX;
import static org.swisspush.redisques.util.RedisquesConfiguration.PROP_PROCESSOR_DELAY_MAX;

public class DefaultRedisquesConfigurationProvider implements RedisquesConfigurationProvider {

    private final Logger log = LoggerFactory.getLogger(DefaultRedisquesConfigurationProvider.class);
    private final Vertx vertx;
    private RedisquesConfiguration redisquesConfiguration;

    private static final Set<String> ALLOWED_CONFIGURATION_VALUES = Stream.of("processorDelayMax")
            .collect(Collectors.toSet());

    public DefaultRedisquesConfigurationProvider(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.redisquesConfiguration = RedisquesConfiguration.fromJsonObject(config);

        vertx.eventBus().consumer(redisquesConfiguration.getConfigurationUpdatedAddress(), (Handler<Message<JsonObject>>) event -> {
            log.info("Received configurations update");
            setConfigurationValues(event.body(), false);
        });
    }

    @Override
    public RedisquesConfiguration configuration() {
        return this.redisquesConfiguration;
    }

    @Override
    public Result<Void, String> updateConfiguration(JsonObject configuration, boolean validateOnly) {
        return setConfigurationValues(configuration, validateOnly);
    }

    private Result<Void, String> setConfigurationValues(JsonObject configurationValues, boolean validateOnly) {
        if (configurationValues != null) {
            List<String> notAllowedConfigurationValues = findNotAllowedConfigurationValues(configurationValues.fieldNames());
            if (notAllowedConfigurationValues.isEmpty()) {
                try {
                    Long processorDelayMaxValue = configurationValues.getLong(PROCESSOR_DELAY_MAX);
                    if (processorDelayMaxValue == null) {
                        return Result.err("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is missing");
                    }
                    if(validateOnly) {
                        vertx.eventBus().publish(configuration().getConfigurationUpdatedAddress(), configurationValues);
                    } else {
                        changeProcessorDelayMax(processorDelayMaxValue);
                        log.info("Updated configuration value of property '{}' to {}", PROCESSOR_DELAY_MAX, processorDelayMaxValue);
                    }
                    return Result.ok(null);
                } catch (ClassCastException ex) {
                    return Result.err("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is not a number");
                }
            } else {
                String notAllowedConfigurationValuesString = notAllowedConfigurationValues.toString();
                return Result.err("Not supported configuration values received: " + notAllowedConfigurationValuesString);
            }
        } else {
            return Result.err("Configuration values missing");
        }
    }

    private List<String> findNotAllowedConfigurationValues(Set<String> configurationValues) {
        if (configurationValues == null) {
            return Collections.emptyList();
        }
        return configurationValues.stream().filter(p -> !ALLOWED_CONFIGURATION_VALUES.contains(p)).collect(Collectors.toList());
    }

    private void changeProcessorDelayMax(long processorDelayMaxValue){
        JsonObject configJsonObject = configuration().asJsonObject();
        configJsonObject.put(PROP_PROCESSOR_DELAY_MAX, processorDelayMaxValue);
        this.redisquesConfiguration = RedisquesConfiguration.fromJsonObject(configJsonObject);
    }
}
