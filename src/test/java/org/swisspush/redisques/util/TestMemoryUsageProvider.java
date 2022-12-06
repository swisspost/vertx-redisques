package org.swisspush.redisques.util;

import java.util.Optional;

public class TestMemoryUsageProvider implements MemoryUsageProvider {

    private Optional<Integer> currentMemoryUsagePercentage;

    public TestMemoryUsageProvider(Optional<Integer> currentMemoryUsagePercentage) {
        this.currentMemoryUsagePercentage = currentMemoryUsagePercentage;
    }

    @Override
    public Optional<Integer> currentMemoryUsagePercentage() {
        return currentMemoryUsagePercentage;
    }

    public void setCurrentMemoryUsage(Optional<Integer> currentMemoryUsagePercentage) {
        this.currentMemoryUsagePercentage = currentMemoryUsagePercentage;
    }
}
