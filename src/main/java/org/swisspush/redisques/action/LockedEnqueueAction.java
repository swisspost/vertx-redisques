package org.swisspush.redisques.action;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.util.MemoryUsageProvider;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.QueueStatisticsCollector;
import org.swisspush.redisques.util.RedisProvider;

import java.util.Arrays;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class LockedEnqueueAction extends EnqueueAction {

    public LockedEnqueueAction(Vertx vertx, RedisProvider redisProvider,
                               String address, String queuesKey, String queuesPrefix,
                               String consumersPrefix, String locksKey, List<QueueConfiguration> queueConfigurations,
                               RedisQuesExceptionFactory exceptionFactory,
                               QueueStatisticsCollector queueStatisticsCollector, Logger log,
                               MemoryUsageProvider memoryUsageProvider, int memoryUsageLimitPercent, MeterRegistry meterRegistry,
                               String metricsIdentifier) {
        super(vertx, redisProvider, address, queuesKey, queuesPrefix, consumersPrefix,
                locksKey, queueConfigurations, exceptionFactory, queueStatisticsCollector, log, memoryUsageProvider,
                memoryUsageLimitPercent, meterRegistry, metricsIdentifier);
    }

    @Override
    public void execute(Message<JsonObject> event) {
        log.debug("RedisQues about to lockedEnqueue");
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        if (isMemoryUsageLimitReached()) {
            log.warn("Failed to lockedEnqueue into queue {} because the memory usage limit is reached", queueName);
            incrEnqueueFailCount();
            event.reply(createErrorReply().put(MESSAGE, MEMORY_FULL));
            return;
        }
        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo != null) {
            var p = redisProvider.redis();
            p.onSuccess(redisAPI -> redisAPI.hmset(Arrays.asList(locksKey, queueName, lockInfo.encode()), putLockResult -> {
                if (putLockResult.succeeded()) {
                    log.debug("RedisQues lockedEnqueue locking successful, now going to enqueue");
                    enqueueActionExecute(event);
                } else {
                    log.warn("RedisQues lockedEnqueue locking failed. Skip enqueue",
                            new Exception(putLockResult.cause()));
                    incrEnqueueFailCount();
                    event.reply(createErrorReply());
                }
            }));
            p.onFailure(ex -> {
                log.warn("Redis: RedisQues lockedEnqueue locking failed. Skip enqueue", new Exception(ex));
                incrEnqueueFailCount();
                event.reply(createErrorReply());
            });
        } else {
            log.warn("RedisQues lockedEnqueue failed because property '{}' was missing", REQUESTED_BY);
            incrEnqueueFailCount();
            event.reply(createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
        }
    }

    private void enqueueActionExecute(Message<JsonObject> event) {
        super.execute(event);
    }
}
