package org.swisspush.redisques;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.handler.RedisquesHttpRequestHandler;
import org.swisspush.redisques.queue.KeyspaceHelper;
import org.swisspush.redisques.queue.QueueActionsService;
import org.swisspush.redisques.queue.QueueConsumerRunner;
import org.swisspush.redisques.queue.QueueMetrics;
import org.swisspush.redisques.queue.QueueRegistryService;
import org.swisspush.redisques.queue.RedisService;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;
import org.swisspush.redisques.util.*;

import java.util.UUID;
import java.util.concurrent.Semaphore;

import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.exception.RedisQuesExceptionFactory.newThriftyExceptionFactory;
import static org.swisspush.redisques.util.RedisquesAPI.*;
import static org.swisspush.redisques.util.RedisquesAPI.QueueOperation.*;

public class RedisQues extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(RedisQues.class);

    // Identifies the consumer
    private final String uid = UUID.randomUUID().toString();
    private final MeterRegistry meterRegistry;

    public String getUid() {
        return uid;
    }

    private RedisService redisService;

    // The queues this verticle instance is registered as a consumer


    private DequeueStatisticCollector dequeueStatisticCollector;
    private QueueStatisticsCollector queueStatisticsCollector;

    // Components
    private RedisProvider redisProvider;
    private QueueActionsService queueActionsService;
    private QueueRegistryService queueRegistryService;
    private QueueMetrics queueMetrics;
    private KeyspaceHelper keyspaceHelper;


    private MemoryUsageProvider memoryUsageProvider;
    private RedisquesConfigurationProvider configurationProvider;
    private RedisMonitor redisMonitor;


    private final RedisQuesExceptionFactory exceptionFactory;
    private PeriodicSkipScheduler periodicSkipScheduler;
    private final Semaphore redisMonitoringReqQuota;
    private final Semaphore checkQueueRequestsQuota;
    private final Semaphore activeQueueRegRefreshReqQuota;

    private final Semaphore queueStatsRequestQuota;
    private final Semaphore getQueuesItemsCountRedisRequestQuota;

    public RedisQues() {
        this(null, null, null, newThriftyExceptionFactory(), new Semaphore(Integer.MAX_VALUE),
                new Semaphore(Integer.MAX_VALUE), new Semaphore(Integer.MAX_VALUE), new Semaphore(Integer.MAX_VALUE),
                new Semaphore(Integer.MAX_VALUE));
        log.warn("Fallback to legacy behavior and allow up to {} simultaneous requests to redis", Integer.MAX_VALUE);
    }

    public RedisQues(
            MemoryUsageProvider memoryUsageProvider,
            RedisquesConfigurationProvider configurationProvider,
            RedisProvider redisProvider,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisMonitoringReqQuota,
            Semaphore activeQueueRegRefreshReqQuota,
            Semaphore checkQueueRequestsQuota,
            Semaphore queueStatsRequestQuota,
            Semaphore getQueuesItemsCountRedisRequestQuota
    ) {
        this(memoryUsageProvider, configurationProvider, redisProvider, exceptionFactory, redisMonitoringReqQuota,
                activeQueueRegRefreshReqQuota, checkQueueRequestsQuota, queueStatsRequestQuota, getQueuesItemsCountRedisRequestQuota, null);
    }

    public RedisQues(
            MemoryUsageProvider memoryUsageProvider,
            RedisquesConfigurationProvider configurationProvider,
            RedisProvider redisProvider,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisMonitoringReqQuota,
            Semaphore activeQueueRegRefreshReqQuota,
            Semaphore checkQueueRequestsQuota,
            Semaphore queueStatsRequestQuota,
            Semaphore getQueuesItemsCountRedisRequestQuota,
            MeterRegistry meterRegistry
    ) {
        this.memoryUsageProvider = memoryUsageProvider;
        this.configurationProvider = configurationProvider;
        this.redisProvider = redisProvider;
        this.exceptionFactory = exceptionFactory;
        this.redisMonitoringReqQuota = redisMonitoringReqQuota;
        this.activeQueueRegRefreshReqQuota = activeQueueRegRefreshReqQuota;
        this.checkQueueRequestsQuota = checkQueueRequestsQuota;
        this.queueStatsRequestQuota = queueStatsRequestQuota;
        this.getQueuesItemsCountRedisRequestQuota = getQueuesItemsCountRedisRequestQuota;
        this.meterRegistry = meterRegistry;

    }

    public static RedisQuesBuilder builder() {
        return new RedisQuesBuilder();
    }


    @Override
    public void start(Promise<Void> promise) {
        log.info("Started with UID {}", uid);

        if (this.configurationProvider == null) {
            this.configurationProvider = new DefaultRedisquesConfigurationProvider(vertx, config());
        }

        if (this.periodicSkipScheduler == null) {
            this.periodicSkipScheduler = new PeriodicSkipScheduler(vertx);
        }
        RedisquesConfiguration modConfig = configurationProvider.configuration();
        log.info("Starting Redisques module with configuration: {}", configurationProvider.configuration());

        if (this.dequeueStatisticCollector == null) {
            this.dequeueStatisticCollector = new DequeueStatisticCollector(vertx, modConfig.isDequeueStatsEnabled());
        }

        if (redisProvider == null) {
            redisProvider = new DefaultRedisProvider(vertx, configurationProvider);
        }
        redisProvider.redis().onComplete(event -> {
            if (event.succeeded()) {
                initialize();
                promise.complete();
            } else {
                promise.fail(new Exception(event.cause()));
            }
        });
        redisService = new RedisService(redisProvider);


    }

    private void initialize() {
        RedisquesConfiguration configuration = configurationProvider.configuration();
        this.keyspaceHelper = new KeyspaceHelper(configuration, uid);

        this.queueStatisticsCollector = new QueueStatisticsCollector(
                redisService, keyspaceHelper.getQueuesPrefix(), vertx, exceptionFactory, redisMonitoringReqQuota,
                configuration.getQueueSpeedIntervalSec());
        this.queueMetrics = new QueueMetrics(vertx, keyspaceHelper, redisService, meterRegistry, configurationProvider, exceptionFactory);

        queueMetrics.initMicrometerMetrics(configuration);
        RedisquesHttpRequestHandler.init(
                vertx, configuration, queueStatisticsCollector, dequeueStatisticCollector,
                exceptionFactory, queueStatsRequestQuota);

        // only initialize memoryUsageProvider when not provided in the constructor
        if (memoryUsageProvider == null) {
            memoryUsageProvider = new DefaultMemoryUsageProvider(redisService, vertx,
                    configurationProvider.configuration().getMemoryUsageCheckIntervalSec());
        }

        assert getQueuesItemsCountRedisRequestQuota != null;

        queueActionsService = new QueueActionsService(vertx, redisService, keyspaceHelper, configurationProvider, exceptionFactory, memoryUsageProvider, queueStatisticsCollector, getQueuesItemsCountRedisRequestQuota, meterRegistry);
        queueRegistryService = new QueueRegistryService(vertx, redisService, configurationProvider, exceptionFactory, keyspaceHelper, queueMetrics, queueStatisticsCollector , checkQueueRequestsQuota, activeQueueRegRefreshReqQuota);

        // Handles operations
        vertx.eventBus().consumer(configurationProvider.configuration().getAddress(), operationsHandler());

        registerMetricsGathering(configuration);
    }

    private Handler<Message<JsonObject>> operationsHandler() {
        return event -> {
            final JsonObject body = event.body();
            if( body == null )
                throw new NullPointerException("Why is body empty? addr=" + event.address() + "  replyAddr=" + event.replyAddress());
            String operation = body.getString(OPERATION);
            log.trace("RedisQues got operation: {}", operation);

            RedisquesAPI.QueueOperation queueOperation = RedisquesAPI.QueueOperation.fromString(operation);
            if (queueOperation == null) {
                unsupportedOperation(operation, event);
                return;
            }
            // handle system operations
            switch (queueOperation) {
                case check:
                    queueRegistryService.checkQueues().onFailure(ex -> {
                        if (log.isWarnEnabled()) {
                            log.warn("TODO error handling", exceptionFactory.newException(ex));
                        }
                    });
                    return;
                case reset:
                    queueRegistryService.resetConsumers();
                    return;
                case stop:
                    queueRegistryService.gracefulStop(aVoid -> {/*no-op*/});
                    return;
            }
            // handle queue action operations
            queueActionsService.handle(queueOperation, event);
        };
    }

    private void unsupportedOperation(String operation, Message<JsonObject> event) {
        JsonObject reply = new JsonObject();
        String message = "QUEUE_ERROR: Unsupported operation received: " + operation;
        log.error(message);
        reply.put(STATUS, ERROR);
        reply.put(MESSAGE, message);
        event.reply(reply);
    }

    private void registerMetricsGathering(RedisquesConfiguration configuration) {
        if (!configuration.getRedisMonitoringEnabled()) {
            return;
        }

        String metricsAddress = configuration.getPublishMetricsAddress();
        if (Strings.isNullOrEmpty(metricsAddress)) {
            return;
        }
        String metricStorageName = configuration.getMetricStorageName();
        int metricRefreshPeriod = configuration.getMetricRefreshPeriod();

        redisMonitor = new RedisMonitor(vertx, redisService, metricsAddress, metricStorageName, metricRefreshPeriod);
        redisMonitor.start();
    }

    public QueueConsumerRunner getQueueConsumerRunner() {
        return queueRegistryService.getQueueConsumerRunner();
    }

    @Override
    public void stop() {
        queueRegistryService.stop();
        if (redisMonitor != null) {
            redisMonitor.stop();
            redisMonitor = null;
        }
    }
}
