package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;

public class DefaultMemoryUsageProvider implements MemoryUsageProvider {

    private Logger log = LoggerFactory.getLogger(DefaultMemoryUsageProvider.class);

    private RedisAPI redisAPI;

    private Optional<Integer> currentMemoryUsagePercentage = Optional.empty();

    private static final int MAX_PERCENTAGE = 100;
    private static final int MIN_PERCENTAGE = 0;

    public DefaultMemoryUsageProvider(RedisAPI redisAPI, Vertx vertx, int memoryUsageCheckIntervalSec) {
        this.redisAPI = redisAPI;
        updateCurrentMemoryUsage();
        vertx.setPeriodic(memoryUsageCheckIntervalSec * 1000L, event -> updateCurrentMemoryUsage());
    }

    public Optional<Integer> currentMemoryUsagePercentage() {
        return currentMemoryUsagePercentage;
    }

    private void updateCurrentMemoryUsage() {
        redisAPI.info(Collections.singletonList("memory")).onComplete(memoryInfoEvent -> {
            if(memoryInfoEvent.failed()) {
                log.error("Unable to get memory information from redis", memoryInfoEvent.cause());
                currentMemoryUsagePercentage = Optional.empty();
                return;
            }

            String memoryInfo = memoryInfoEvent.result().toString();

            Optional<Long> totalSystemMemory = totalSystemMemory(memoryInfo);
            if (totalSystemMemory.isEmpty()) {
                currentMemoryUsagePercentage = Optional.empty();
                return;
            }

            Optional<Long> usedMemory = usedMemory(memoryInfo);
            if (usedMemory.isEmpty()) {
                currentMemoryUsagePercentage = Optional.empty();
                return;
            }

            float currentMemoryUsagePercentage = ((float) usedMemory.get() / totalSystemMemory.get()) * 100;
            if (currentMemoryUsagePercentage > MAX_PERCENTAGE) {
                currentMemoryUsagePercentage = MAX_PERCENTAGE;
            } else if (currentMemoryUsagePercentage < MIN_PERCENTAGE) {
                currentMemoryUsagePercentage = MIN_PERCENTAGE;
            }

            int roundedValue = Math.round(currentMemoryUsagePercentage);
            log.info("Current memory usage is {}%", roundedValue);
            this.currentMemoryUsagePercentage = Optional.of(roundedValue);
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
        log.warn("No or invalid '{}' value received from redis. Unable to calculate the current memory usage.", property, ex);
    }
}
