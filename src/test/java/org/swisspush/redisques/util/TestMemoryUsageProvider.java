package org.swisspush.redisques.util;

import java.util.Optional;

public class TestMemoryUsageProvider implements MemoryUsageProvider {

    private Optional<Float> currentMemoryUsage;

    public TestMemoryUsageProvider(Optional<Float> currentMemoryUsage) {
        this.currentMemoryUsage = currentMemoryUsage;
    }

    @Override
    public Optional<Float> currentMemoryUsage() {
        return currentMemoryUsage;
    }

    public void setCurrentMemoryUsage(Optional<Float> currentMemoryUsage) {
        this.currentMemoryUsage = currentMemoryUsage;
    }
}
