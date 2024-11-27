package org.swisspush.redisques.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.util.MetricMeter;

import java.util.concurrent.atomic.AtomicLong;

import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.VALUE;
import static org.swisspush.redisques.util.RedisquesAPI.MESSAGE;
import static org.swisspush.redisques.util.RedisquesAPI.buildGetQueuesCountOperation;


public class PeriodicMetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(PeriodicMetricsCollector.class);
    private final Vertx vertx;
    private final String redisquesAddress;

    private final AtomicLong activeQueuesCount = new AtomicLong(0);

    public PeriodicMetricsCollector(Vertx vertx, String redisquesAddress, MeterRegistry meterRegistry, long metricCollectIntervalSec) {
        this.vertx = vertx;
        this.redisquesAddress = redisquesAddress;

        Gauge.builder(MetricMeter.ACTIVE_QUEUES.getId(), activeQueuesCount, AtomicLong::get).
                description(MetricMeter.ACTIVE_QUEUES.getDescription()).
                register(meterRegistry);

        vertx.setPeriodic(metricCollectIntervalSec * 1000, event -> updateActiveQueuesCount());
    }

    private void updateActiveQueuesCount() {
        vertx.eventBus().request(redisquesAddress, buildGetQueuesCountOperation(), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if(reply.failed()) {
                log.warn("TODO error handling", reply.cause());
            } else if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                activeQueuesCount.set(reply.result().body().getLong(VALUE));
            } else {
                log.warn("Error gathering count of active queues. Cause: {}", reply.result().body().getString(MESSAGE));
            }
        });
    }

}
