package org.swisspush.redisques.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.util.internal.StringUtil;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.lock.Lock;
import org.swisspush.redisques.util.LockUtil;
import org.swisspush.redisques.util.MetricMeter;
import org.swisspush.redisques.util.MetricTags;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    private final Vertx vertx;
    private final String redisquesAddress;
    private final Lock lock;
    private final String uid;
    private final long metricCollectIntervalMs;

    private static final String DEFAULT_IDENTIFIER = "default";
    private final String UPDATE_ACTIVE_QUEUES_LOCK;
    private final String UPDATE_MAX_QUEUE_SIZE_LOCK;

    private final AtomicLong activeQueuesCount = new AtomicLong(0);
    private final AtomicLong maxQueueSize = new AtomicLong(0);

    public MetricsCollector(Vertx vertx, String uid, String redisquesAddress,
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
        UPDATE_ACTIVE_QUEUES_LOCK = "updateActiveQueuesLock_" + hostName + "_"  + id;
        UPDATE_MAX_QUEUE_SIZE_LOCK = "updateMaxQueueSizeLock_" + hostName + "_"  + id;

        Gauge.builder(MetricMeter.ACTIVE_QUEUES.getId(), activeQueuesCount, AtomicLong::get)
                .tag(MetricTags.IDENTIFIER.getId(), id)
                .description(MetricMeter.ACTIVE_QUEUES.getDescription())
                .register(meterRegistry);

        Gauge.builder(MetricMeter.MAX_QUEUE_SIZE.getId(), maxQueueSize, AtomicLong::get)
                .tag(MetricTags.IDENTIFIER.getId(), id)
                .description(MetricMeter.MAX_QUEUE_SIZE.getDescription())
                .register(meterRegistry);

    }

    public Future<Void> updateActiveQueuesCount() {
        final Promise<Void> promise = Promise.promise();
        acquireLock(UPDATE_ACTIVE_QUEUES_LOCK, createToken(UPDATE_ACTIVE_QUEUES_LOCK)).onComplete(lockEvent -> {
            if(lockEvent.result()) {
                log.info("About to update queues count with lock {}", UPDATE_ACTIVE_QUEUES_LOCK);
                vertx.eventBus().request(redisquesAddress, buildGetQueuesCountOperation(), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                    if (reply.failed()) {
                        log.warn("TODO error handling", reply.cause());
                    } else if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                        activeQueuesCount.set(reply.result().body().getLong(VALUE));
                    } else {
                        log.warn("Error gathering count of active queues. Cause: {}", reply.result().body().getString(MESSAGE));
                    }
                    promise.complete();
                });
            } else {
                promise.complete();
            }
        });
        return promise.future();
    }

    public Future<Void> updateMaxQueueSize() {
        final Promise<Void> promise = Promise.promise();
        acquireLock(UPDATE_MAX_QUEUE_SIZE_LOCK, createToken(UPDATE_MAX_QUEUE_SIZE_LOCK)).onComplete(lockEvent -> {
            if(lockEvent.result()) {
                log.info("About to update max queue size with lock {}", UPDATE_MAX_QUEUE_SIZE_LOCK);
                vertx.eventBus().request(redisquesAddress, buildMonitorOperation(true, 1), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                    if (reply.failed()) {
                        log.warn("TODO error handling", reply.cause());
                    } else if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                        extractMaxQueueSize(reply.result().body());
                    } else {
                        log.warn("Error gathering max queue size. Cause: {}", reply.result().body().getString(MESSAGE));
                    }
                    promise.complete();
                });
            } else {
                promise.complete();
            }
        });
        return promise.future();
    }

    private void extractMaxQueueSize(JsonObject result){
        JsonObject valueObj = result.getJsonObject(VALUE);
        if (valueObj == null) {
            log.warn("No value found in monitor result. Set max queue size to 0");
            maxQueueSize.set(0);
            return;
        }
        JsonArray queuesJsonArr = valueObj.getJsonArray(QUEUES);
        if (queuesJsonArr == null || queuesJsonArr.isEmpty()) {
            log.debug("No queues found. Set max queue size to 0");
            maxQueueSize.set(0);
            return;
        }

        JsonObject item0 = queuesJsonArr.getJsonObject(0);
        if(item0 != null) {
            Long queueSize = item0.getLong(MONITOR_QUEUE_SIZE);
            if(queueSize != null) {
                maxQueueSize.set(queueSize);
            } else {
                log.warn("No queue size found in queue entry. Set max queue size to 0");
                maxQueueSize.set(0);
            }
        }

    }

    private Future<Boolean> acquireLock(String lock, String token) {
        final Promise<Boolean> promise = Promise.promise();
        LockUtil.acquireLock(this.lock, lock, token, LockUtil.calcLockExpiry(metricCollectIntervalMs), log).onComplete(lockEvent -> {
            if (lockEvent.succeeded()) {
                if (lockEvent.result()) {
                    promise.complete(Boolean.TRUE);
                } else {
                    promise.complete(Boolean.FALSE);
                }
            } else {
                log.error("Could not acquire lock '{}'. Message: {}", lock, lockEvent.cause().getMessage());
                promise.complete(Boolean.FALSE);
            }
        });
        return promise.future();
    }

    private String createToken(String appendix) {
        return this.uid + "_" + System.currentTimeMillis() + "_" + appendix;
    }
}
