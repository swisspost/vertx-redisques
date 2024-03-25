package org.swisspush.redisques.util;

public interface MetricsPublisher {

    void publishMetric(String name, long value);
}
