package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.queue.RedisService;

import java.util.Collections;
import java.util.Optional;

public class DefaultMemoryUsageProvider implements MemoryUsageProvider {

    private final Logger log = LoggerFactory.getLogger(DefaultMemoryUsageProvider.class);

    private final RedisService redisService;

    private Optional<Integer> currentMemoryUsagePercentageOpt = Optional.empty();

    private static final int MAX_PERCENTAGE = 100;
    private static final int MIN_PERCENTAGE = 0;

    public DefaultMemoryUsageProvider(RedisService redisService, Vertx vertx, int memoryUsageCheckIntervalSec) {
        this.redisService = redisService;
        updateCurrentMemoryUsage();
        vertx.setPeriodic(memoryUsageCheckIntervalSec * 1000L, event -> updateCurrentMemoryUsage());
    }

    public Optional<Integer> currentMemoryUsagePercentage() {
        return currentMemoryUsagePercentageOpt;
    }

    private void updateCurrentMemoryUsage() {
        redisService.info(Collections.singletonList("memory"))
                .onComplete(memoryInfoEvent -> {
                    if (memoryInfoEvent.failed()) {
                        log.error("Unable to get memory information from redis", memoryInfoEvent.cause());
                        currentMemoryUsagePercentageOpt = Optional.empty();
                        return;
                    }

                    String memoryInfo = memoryInfoEvent.result().toString();

                    Optional<Long> availableMemory = availableMemory(memoryInfo);
                    if (availableMemory.isEmpty()) {
                        currentMemoryUsagePercentageOpt = Optional.empty();
                        return;
                    }

                    Optional<Long> usedMemory = evaluateProperty(memoryInfo, "used_memory", true);
                    if (usedMemory.isEmpty()) {
                        currentMemoryUsagePercentageOpt = Optional.empty();
                        return;
                    }

                    float currentMemoryUsagePercentage = ((float) usedMemory.get() / availableMemory.get()) * 100;
                    if (currentMemoryUsagePercentage > MAX_PERCENTAGE) {
                        currentMemoryUsagePercentage = MAX_PERCENTAGE;
                    } else if (currentMemoryUsagePercentage < MIN_PERCENTAGE) {
                        currentMemoryUsagePercentage = MIN_PERCENTAGE;
                    }

                    int roundedValue = Math.round(currentMemoryUsagePercentage);
                    log.info("Current memory usage is {}%", roundedValue);
                    currentMemoryUsagePercentageOpt = Optional.of(roundedValue);
                });
    }

    /**
     * Evaluate the available memory based on multiple possible redis configuration options.
     * First try with 'maxmemory' and if not available try with 'total_system_memory' because the latter
     * is not available anymore on AWS MemoryDB setups.
     */
    private Optional<Long> availableMemory(String memoryInfo) {
        Optional<Long> availableMemoryOpt = evaluateProperty(memoryInfo, "maxmemory", false);
        if (availableMemoryOpt.isEmpty()) {
            log.trace("No 'maxmemory' available. Try with 'total_system_memory' section.");
            availableMemoryOpt = evaluateProperty(memoryInfo, "total_system_memory", false);
            if (availableMemoryOpt.isEmpty()) {
                log.debug("No 'maxmemory' or 'total_system_memory' available. Unable to calculate the current memory usage");
                return Optional.empty();
            }
        }
        return availableMemoryOpt;
    }

    private Optional<Long> evaluateProperty(String memoryInfo, String property, boolean allowZero) {
        try {
            Optional<String> propertyOpt = memoryInfo
                    .lines()
                    .filter(source -> source.startsWith(property + ":"))
                    .findAny();
            if (propertyOpt.isEmpty()) {
                log.trace("No property '{}' section received from redis.", property);
                return Optional.empty();
            }
            long value = Long.parseLong(propertyOpt.get().split(":")[1]);
            if (!allowZero && value == 0L) {
                log.trace("Property '{}' value 0 received from redis", property);
                return Optional.empty();
            }
            return Optional.of(value);
        } catch (NumberFormatException ex) {
            logPropertyWarning(property, ex);
            return Optional.empty();
        }
    }

    private void logPropertyWarning(String property, Exception ex) {
        log.warn("No or invalid '{}' value received from redis. Unable to calculate the current memory usage.", property, ex);
    }
}
