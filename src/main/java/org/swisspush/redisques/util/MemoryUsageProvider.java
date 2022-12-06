package org.swisspush.redisques.util;

import java.util.Optional;

public interface MemoryUsageProvider {

    Optional<Integer> currentMemoryUsagePercentage();
}
