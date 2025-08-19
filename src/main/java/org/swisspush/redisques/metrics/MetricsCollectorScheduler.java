package org.swisspush.redisques.metrics;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCollectorScheduler {

    private final MetricsCollector metricsCollector;
    private static final Logger log = LoggerFactory.getLogger(MetricsCollectorScheduler.class);

    public MetricsCollectorScheduler(Vertx vertx, MetricsCollector metricsCollector, long collectIntervalSec) {
        this.metricsCollector = metricsCollector;

        long collectIntervalMs = collectIntervalSec * 1000;

        vertx.setPeriodic(collectIntervalMs, event -> {
            updateActiveQueuesCount();
            updateMaxQueueSize();
            updateMyQueuesStateCount();
        });
    }

    private void updateActiveQueuesCount() {
        metricsCollector.updateActiveQueuesCount().onComplete(updateEvent -> {
            if(updateEvent.failed()) {
                log.warn("Failed to update active queues count", updateEvent.cause());
            }
        });
    }

    private void updateMaxQueueSize() {
        metricsCollector.updateMaxQueueSize().onComplete(updateEvent -> {
            if(updateEvent.failed()) {
                log.warn("Failed to update max queue size", updateEvent.cause());
            }
        });
    }

    private void updateMyQueuesStateCount() {
        metricsCollector.updateMyQueuesStateCount().onComplete(updateEvent -> {
            if(updateEvent.failed()) {
                log.warn("Failed to update queue state count", updateEvent.cause());
            }
        });
    }
}
