package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Optional;

public class DefaultMemoryUsageProvider implements MemoryUsageProvider {

    private Logger log = LoggerFactory.getLogger(DefaultMemoryUsageProvider.class);

    private RedisAPI redisAPI;

    private Optional<Float> currentMemoryUsageOptional = Optional.empty();

    private static final float MAX_PERCENTAGE = 100.0f;
    private static final float MIN_PERCENTAGE = 0.0f;

    public DefaultMemoryUsageProvider(RedisAPI redisAPI, Vertx vertx, int memoryUsageCheckIntervalSec) {
        this.redisAPI = redisAPI;
        updateCurrentMemoryUsage();
        vertx.setPeriodic(memoryUsageCheckIntervalSec * 1000L, event -> updateCurrentMemoryUsage());
    }

    public Optional<Float> currentMemoryUsage() {
        return currentMemoryUsageOptional;
    }

    private void updateCurrentMemoryUsage() {
        redisAPI.info(Collections.singletonList("memory")).onComplete(memoryInfoEvent -> {
            if(memoryInfoEvent.failed()) {
                log.error("Unable to get memory information from redis", memoryInfoEvent.cause());
                currentMemoryUsageOptional = Optional.empty();
                return;
            }

            String memoryInfo = memoryInfoEvent.result().toString();

            Optional<Long> totalSystemMemory = totalSystemMemory(memoryInfo);
            if (totalSystemMemory.isEmpty()) {
                currentMemoryUsageOptional = Optional.empty();
                return;
            }

            Optional<Long> usedMemory = usedMemory(memoryInfo);
            if (usedMemory.isEmpty()) {
                currentMemoryUsageOptional = Optional.empty();
                return;
            }

            float currentMemoryUsagePercentage = ((float) usedMemory.get() / totalSystemMemory.get()) * 100;
            if (currentMemoryUsagePercentage > MAX_PERCENTAGE) {
                currentMemoryUsagePercentage = MAX_PERCENTAGE;
            } else if (currentMemoryUsagePercentage < MIN_PERCENTAGE) {
                currentMemoryUsagePercentage = MIN_PERCENTAGE;
            }
            currentMemoryUsagePercentage = new BigDecimal(currentMemoryUsagePercentage)
                    .setScale(2, RoundingMode.HALF_UP)
                    .floatValue();

            log.info("Current memory usage is {}%", currentMemoryUsagePercentage);
            currentMemoryUsageOptional = Optional.of(currentMemoryUsagePercentage);
        });
    }

    private Optional<Long> totalSystemMemory(String memoryInfo) {
        long totalSystemMemory;
        try {
            Optional<String> totalSystemMemoryOpt = memoryInfo
                    .lines()
                    .filter(source -> source.startsWith("total_system_memory:"))
                    .findAny();
            if (totalSystemMemoryOpt.isEmpty()) {
                log.warn("No 'total_system_memory' section received from redis. Unable to calculate the current memory usage");
                return Optional.empty();
            }
            totalSystemMemory = Long.parseLong(totalSystemMemoryOpt.get().split(":")[1]);
            if (totalSystemMemory == 0L) {
                log.warn("'total_system_memory' value 0 received from redis. Unable to calculate the current memory usage");
                return Optional.empty();
            }

        } catch (NumberFormatException ex) {
            logPropertyWarning("total_system_memory", ex);
            return Optional.empty();
        }

        return Optional.of(totalSystemMemory);
    }

    private Optional<Long> usedMemory(String memoryInfo) {
        try {
            Optional<String> usedMemoryOpt = memoryInfo
                    .lines()
                    .filter(source -> source.startsWith("used_memory:"))
                    .findAny();
            if (usedMemoryOpt.isEmpty()) {
                log.warn("No 'used_memory' section received from redis. Unable to calculate the current memory usage");
                return Optional.empty();
            }
            return Optional.of(Long.parseLong(usedMemoryOpt.get().split(":")[1]));
        } catch (NumberFormatException ex) {
            logPropertyWarning("used_memory", ex);
            return Optional.empty();
        }
    }

    private void logPropertyWarning(String property, Exception ex) {
        log.warn("No or invalid '{}' value received from redis. Unable to calculate the current memory usage. " +
                "Exception: {}", property, ex.toString());
    }
}
