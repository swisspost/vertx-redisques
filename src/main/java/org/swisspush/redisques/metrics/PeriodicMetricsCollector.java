package org.swisspush.redisques.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.util.internal.StringUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.lock.Lock;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.LockUtil;
import org.swisspush.redisques.util.MetricMeter;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
    private Lock lock;
    private final String uid;
    private final long metricCollectIntervalMs;

    private static final String DEFAULT_IDENTIFIER = "default";
    private final String UPDATE_METRICS_LOCK;

    private final AtomicLong activeQueuesCount = new AtomicLong(0);

    public PeriodicMetricsCollector(Vertx vertx, String uid, PeriodicSkipScheduler periodicSkipScheduler, String redisquesAddress,
                                    String identifier, MeterRegistry meterRegistry, Lock lock, long metricCollectIntervalSec) {
        this.vertx = vertx;
        this.uid = uid;
        this.redisquesAddress = redisquesAddress;
        this.lock = lock;
        this.metricCollectIntervalMs = metricCollectIntervalSec * 1000;

        String id = identifier;

        if (StringUtil.isNullOrEmpty(id)) {
            id = DEFAULT_IDENTIFIER;
        }

        String hostName = "unknown";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        UPDATE_METRICS_LOCK = "updateMetricsLock_" + hostName + "_"  + id;

        Gauge.builder(MetricMeter.ACTIVE_QUEUES.getId(), activeQueuesCount, AtomicLong::get).tag("identifier", id)
                .description(MetricMeter.ACTIVE_QUEUES.getDescription()).register(meterRegistry);

        periodicSkipScheduler.setPeriodic(metricCollectIntervalMs, "metricCollectRefresh",
                this::updateActiveQueuesCount);
    }

    private void updateActiveQueuesCount(Runnable onPeriodicDone) {
        final String token = createToken(UPDATE_METRICS_LOCK);
        LockUtil.acquireLock(this.lock, UPDATE_METRICS_LOCK, token, LockUtil.calcLockExpiry(metricCollectIntervalMs), log).onComplete(lockEvent -> {
            if (lockEvent.succeeded()) {
                if (lockEvent.result()) {
                    log.info("About to update queues count with lock {}", UPDATE_METRICS_LOCK);
                    vertx.eventBus().request(redisquesAddress, buildGetQueuesCountOperation(), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                        if (reply.failed()) {
                            log.warn("TODO error handling", reply.cause());
                        } else if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                            activeQueuesCount.set(reply.result().body().getLong(VALUE));
                        } else {
                            log.warn("Error gathering count of active queues. Cause: {}", reply.result().body().getString(MESSAGE));
                        }
                        onPeriodicDone.run();
                    });
                } else {
                    onPeriodicDone.run();
                }
            } else {
                log.error("Could not acquire lock '{}'. Message: {}", UPDATE_METRICS_LOCK, lockEvent.cause().getMessage());
                onPeriodicDone.run();
            }
        });
    }

    private String createToken(String appendix) {
        return this.uid + "_" + System.currentTimeMillis() + "_" + appendix;
    }
}
