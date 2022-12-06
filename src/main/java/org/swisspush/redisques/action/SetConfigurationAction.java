package org.swisspush.redisques.action;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class SetConfigurationAction implements QueueAction {

    private static final Set<String> ALLOWED_CONFIGURATION_VALUES = Stream.of("processorDelayMax")
            .collect(Collectors.toSet());

    protected Logger log;

    protected Vertx vertx;
    protected String configurationUpdatedAddress;

    public SetConfigurationAction(Logger log, Vertx vertx, String configurationUpdatedAddress) {
        this.log = log;
        this.vertx = vertx;
        this.configurationUpdatedAddress = configurationUpdatedAddress;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject configurationValues = event.body().getJsonObject(PAYLOAD);
        setConfigurationValues(configurationValues, true).onComplete(setConfigurationValuesEvent -> {
            if (setConfigurationValuesEvent.succeeded()) {
                log.debug("About to publish the configuration updates to event bus address '" + configurationUpdatedAddress + "'");
                vertx.eventBus().publish(configurationUpdatedAddress, configurationValues);
                event.reply(setConfigurationValuesEvent.result());
            } else {
                event.reply(QueueAction.createErrorReply().put(MESSAGE, setConfigurationValuesEvent.cause().getMessage()));
            }
        });
    }

    private Future<JsonObject> setConfigurationValues(JsonObject configurationValues, boolean validateOnly) {
        Promise<JsonObject> promise = Promise.promise();

        if (configurationValues != null) {
            List<String> notAllowedConfigurationValues = findNotAllowedConfigurationValues(configurationValues.fieldNames());
            if (notAllowedConfigurationValues.isEmpty()) {
                try {
                    Long processorDelayMaxValue = configurationValues.getLong(PROCESSOR_DELAY_MAX);
                    if (processorDelayMaxValue == null) {
                        promise.fail("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is missing");
                        return promise.future();
                    }
                    if (!validateOnly) {
                        //FIXME complete impl. find a way to set processorDelayMax
//                        this.processorDelayMax = processorDelayMaxValue;
                        log.info("Updated configuration value of property '" + PROCESSOR_DELAY_MAX + "' to " + processorDelayMaxValue);
                    }
                    promise.complete(QueueAction.createOkReply());
                } catch (ClassCastException ex) {
                    promise.fail("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is not a number");
                }
            } else {
                String notAllowedConfigurationValuesString = notAllowedConfigurationValues.toString();
                promise.fail("Not supported configuration values received: " + notAllowedConfigurationValuesString);
            }
        } else {
            promise.fail("Configuration values missing");
        }

        return promise.future();
    }

    private List<String> findNotAllowedConfigurationValues(Set<String> configurationValues) {
        if (configurationValues == null) {
            return Collections.emptyList();
        }
        return configurationValues.stream().filter(p -> !ALLOWED_CONFIGURATION_VALUES.contains(p)).collect(Collectors.toList());
    }
}
