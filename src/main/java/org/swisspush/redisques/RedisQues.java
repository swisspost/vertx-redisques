package org.swisspush.redisques;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.op.RangeLimitOptions;
import io.vertx.redis.op.SetOptions;
import org.swisspush.redisques.handler.*;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.MessageUtil;
import org.swisspush.redisques.util.RedisQuesTimer;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.Result;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.swisspush.redisques.util.RedisquesAPI.*;

public class RedisQues extends AbstractVerticle {

    // State of each queue. Consuming means there is a message being processed.
    private enum QueueState {
        READY, CONSUMING
    }

    // Identifies the consumer
    private String uid = UUID.randomUUID().toString();

    private MessageConsumer<String> uidMessageConsumer;

    // The queues this verticle is listening to
    private Map<String, QueueState> myQueues = new HashMap<>();

    private Logger log = LoggerFactory.getLogger(RedisQues.class);

    private Handler<Void> stoppedHandler = null;

    private MessageConsumer<String> conumersMessageConsumer;

    // Configuration

    // Address of this redisques. Also used as prefix for consumer broadcast
    // address.
    private String address = "redisques";

    private String configurationUpdatedAddress = "redisques-configuration-updated";

    // Address of the redis mod
    private RedisClient redisClient;

    // Prefix for redis keys holding queues and consumers.
    private String redisPrefix = "redisques:";

    private String getQueuesPrefix() {
        return redisPrefix + "queues:";
    }

    private String getQueuesKey() {
        return redisPrefix + "queues";
    }

    public String getConsumersPrefix() {
        return redisPrefix + "consumers:";
    }

    private String getLocksKey() {
        return redisPrefix + "locks";
    }

    private String getQueueCheckLastexecKey() {
        return redisPrefix + "check:lastexec";
    }
    
    private String getQueueProcessMessageFailureCountPrefix() {
        return redisPrefix + "process:message:failure:count:";
    }

    // Address of message processors
    private String processorAddress = "redisques-processor";

    public static final String TIMESTAMP = "timestamp";

    // Consumers periodically refresh their subscription while they are
    // consuming.
    private int refreshPeriod;
    private int slowDownExtension;
    private int maxSlowDown;
    private int consumerLockTime;

    private int checkInterval;

    // the time we wait for the processor to answer, before we cancel processing
    private int processorTimeout = 240000;

    private long processorDelayMax;
    private RedisQuesTimer timer;

    private String redisHost;
    private int redisPort;
    private String redisAuth;
    private String redisEncoding;

    private boolean httpRequestHandlerEnabled;
    private String httpRequestHandlerPrefix;
    private int httpRequestHandlerPort;
    private String httpRequestHandlerUserHeader;

    private static final int DEFAULT_MAX_QUEUEITEM_COUNT = 49;
    private static final int MAX_AGE_MILLISECONDS = 120000; // 120 seconds

    private static final Set<String> ALLOWED_CONFIGURATION_VALUES = Stream.of("processorDelayMax").collect(Collectors.toSet());

    private LuaScriptManager luaScriptManager;

    // Handler receiving registration requests when no consumer is registered
    // for a queue.
    private Handler<Message<String>> registrationRequestHandler = event -> {
        final String queue = event.body();
        log.debug("RedisQues Got registration request for queue " + queue + " from consumer: " + uid);
        // Try to register for this queue
        SetOptions setOptions = new SetOptions().setNX(true).setEX(consumerLockTime);
        redisClient.setWithOptions(getConsumersPrefix() + queue, uid, setOptions, event1 -> {
            if (event1.succeeded()) {
                String value = event1.result();
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues setxn result: " + value + " for queue: " + queue);
                }
                if ("OK".equals(value)) {
                    // I am now the registered consumer for this queue.
                    log.debug("RedisQues Now registered for queue " + queue);
                    myQueues.put(queue, QueueState.READY);
                    consume(queue);
                } else {
                    log.debug("RedisQues Missed registration for queue " + queue);
                    // Someone else just became the registered consumer. I
                    // give up.
                }
            } else {
                log.error("RedisQues setxn failed", event1.cause());
            }
        });
    };

    @Override
    public void start() {
        final EventBus eb = vertx.eventBus();
        log.info("Started with UID " + uid);

        RedisquesConfiguration modConfig = RedisquesConfiguration.fromJsonObject(config());
        log.info("Starting Redisques module with configuration: " + modConfig);

        address = modConfig.getAddress();
        configurationUpdatedAddress = modConfig.getConfigurationUpdatedAddress();
        redisPrefix = modConfig.getRedisPrefix();
        processorAddress = modConfig.getProcessorAddress();
        refreshPeriod = modConfig.getRefreshPeriod();
        slowDownExtension = modConfig.getSlowDownExtension();
        maxSlowDown = modConfig.getMaxSlowDown();
        consumerLockTime = 2 * refreshPeriod; // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        checkInterval = modConfig.getCheckInterval();
        processorTimeout = modConfig.getProcessorTimeout();
        processorDelayMax = modConfig.getProcessorDelayMax();
        timer = new RedisQuesTimer(vertx);

        redisHost = modConfig.getRedisHost();
        redisPort = modConfig.getRedisPort();
        redisAuth = modConfig.getRedisAuth();
        redisEncoding = modConfig.getRedisEncoding();

        httpRequestHandlerEnabled = modConfig.getHttpRequestHandlerEnabled();
        httpRequestHandlerPrefix = modConfig.getHttpRequestHandlerPrefix();
        httpRequestHandlerPort = modConfig.getHttpRequestHandlerPort();
        httpRequestHandlerUserHeader = modConfig.getHttpRequestHandlerUserHeader();

        this.redisClient = RedisClient.create(vertx, new RedisOptions()
                .setHost(redisHost)
                .setPort(redisPort)
                .setAuth(redisAuth)
                .setEncoding(redisEncoding));

        this.luaScriptManager = new LuaScriptManager(redisClient);

        RedisquesHttpRequestHandler.init(vertx, modConfig);

        eb.consumer(configurationUpdatedAddress, (Handler<Message<JsonObject>>) event -> {
            log.info("Received configurations update");
            setConfigurationValues(event.body(), false);
        });

        // Handles operations
        eb.localConsumer(address, (Handler<Message<JsonObject>>) event -> {
            String operation = event.body().getString(OPERATION);
            if (log.isTraceEnabled()) {
                log.trace("RedisQues got operation:" + operation);
            }

            QueueOperation queueOperation = QueueOperation.fromString(operation);
            if (queueOperation == null) {
                unsupportedOperation(operation, event);
                return;
            }

            switch (queueOperation) {
                case enqueue:
                    enqueue(event);
                    break;
                case lockedEnqueue:
                    lockedEnqueue(event);
                    break;
                case getQueueItems:
                    getQueueItems(event);
                    break;
                case addQueueItem:
                    addQueueItem(event);
                    break;
                case deleteQueueItem:
                    deleteQueueItem(event);
                    break;
                case getQueueItem:
                    getQueueItem(event);
                    break;
                case replaceQueueItem:
                    replaceQueueItem(event);
                    break;
                case deleteAllQueueItems:
                    deleteAllQueueItems(event);
                    break;
                case bulkDeleteQueues:
                    bulkDeleteQueues(event);
                case getAllLocks:
                    getAllLocks(event);
                    break;
                case putLock:
                    putLock(event);
                    break;
                case bulkPutLocks:
                    bulkPutLocks(event);
                    break;
                case getLock:
                    redisClient.hget(getLocksKey(), event.body().getJsonObject(PAYLOAD).getString(QUEUENAME), new GetLockHandler(event));
                    break;
                case deleteLock:
                    deleteLock(event);
                    break;
                case bulkDeleteLocks:
                    bulkDeleteLocks(event);
                    break;
                case deleteAllLocks:
                    deleteAllLocks(event);
                    break;
                case getQueueItemsCount:
                    redisClient.llen(getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME), new GetQueueItemsCountHandler(event));
                    break;
                case getQueuesCount:
                    getQueuesCount(event);
                    break;
                case getQueues:
                    getQueues(event, false);
                    break;
                case check:
                    checkQueues();
                    break;
                case reset:
                    resetConsumers();
                    break;
                case stop:
                    gracefulStop(event1 -> {
                        JsonObject reply = new JsonObject();
                        reply.put(STATUS, OK);
                    });
                    break;
                case getConfiguration:
                    getConfiguration(event);
                    break;
                case setConfiguration:
                    setConfiguration(event);
                    break;
                default:
                    unsupportedOperation(operation, event);
            }
        });

        // Handles registration requests
        conumersMessageConsumer = eb.consumer(address + "-consumers", registrationRequestHandler);

        // Handles notifications
        uidMessageConsumer = eb.consumer(uid, event -> {
            final String queue = event.body();
            log.debug("RedisQues Got notification for queue " + queue);
            consume(queue);
        });

        // Periodic refresh of my registrations on active queues.
        vertx.setPeriodic(refreshPeriod * 1000, event -> {
            // Check if I am still the registered consumer
            myQueues.entrySet().stream().filter(entry -> entry.getValue() == QueueState.CONSUMING).forEach(entry -> {
                final String queue = entry.getKey();
                // Check if I am still the registered consumer
                String consumerKey = getConsumersPrefix() + queue;
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues refresh queues get: " + consumerKey);
                }
                redisClient.get(consumerKey, event1 -> {
                    String consumer = event1.result();
                    if (uid.equals(consumer)) {
                        log.debug("RedisQues Periodic consumer refresh for active queue " + queue);
                        refreshRegistration(queue, null);
                        updateTimestamp(queue, null);
                    } else {
                        log.debug("RedisQues Removing queue " + queue + " from the list");
                        myQueues.remove(queue);
                    }
                });
            });
        });

        registerQueueCheck(modConfig);
    }

    private void enqueue(Message<JsonObject> event) {
        updateTimestamp(event.body().getJsonObject(PAYLOAD).getString(QUEUENAME), null);
        String keyEnqueue = getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        String valueEnqueue = event.body().getString(MESSAGE);
        redisClient.rpush(keyEnqueue, valueEnqueue, event2 -> {
            JsonObject reply = new JsonObject();
            if (event2.succeeded()) {
                log.debug("RedisQues Enqueued message into queue " + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME));
                notifyConsumer(event.body().getJsonObject(PAYLOAD).getString(QUEUENAME));
                reply.put(STATUS, OK);
                reply.put(MESSAGE, "enqueued");
                event.reply(reply);
            } else {
                String message = "RedisQues QUEUE_ERROR: Error while enqueueing message into queue " + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
                log.error(message);
                reply.put(STATUS, ERROR);
                reply.put(MESSAGE, message);
                event.reply(reply);
            }
        });
    }

    private void lockedEnqueue(Message<JsonObject> event) {
        log.debug("RedisQues about to lockedEnqueue");
        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo != null) {
            redisClient.hmset(getLocksKey(), new JsonObject().put(event.body().getJsonObject(PAYLOAD).getString(QUEUENAME), lockInfo.encode()),
                    putLockResult -> {
                        if (putLockResult.succeeded()) {
                            log.debug("RedisQues lockedEnqueue locking successful, now going to enqueue");
                            enqueue(event);
                        } else {
                            log.warn("RedisQues lockedEnqueue locking failed. Skip enqueue");
                            event.reply(new JsonObject().put(STATUS, ERROR));
                        }
                    });
        } else {
            log.warn("RedisQues lockedEnqueue failed because property '" + REQUESTED_BY + "' was missing");
            event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
        }

    }

    private void addQueueItem(Message<JsonObject> event) {
        String key1 = getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        String valueAddItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        redisClient.rpush(key1, valueAddItem, new AddQueueItemHandler(event));
    }

    private void getQueueItems(Message<JsonObject> event) {
        String keyListRange = getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int maxQueueItemCountIndex = getMaxQueueItemCountIndex(event.body().getJsonObject(PAYLOAD).getString(LIMIT));
        redisClient.llen(keyListRange, countReply -> {
            Long queueItemCount = countReply.result();
            if (countReply.succeeded() && queueItemCount != null) {
                redisClient.lrange(keyListRange, 0, maxQueueItemCountIndex, new GetQueueItemsHandler(event, queueItemCount));
            } else {
                log.warn("RedisQues getQueueItems failed", countReply.cause());
            }
        });
    }

    private void getQueues(Message<JsonObject> event, boolean countOnly) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        getQueues(event, countOnly, result);
    }

    private void getQueues(Message<JsonObject> event, boolean countOnly, Result<Optional<Pattern>, String> filterPatternResult) {
        if (filterPatternResult.isErr()) {
            event.reply(new JsonObject().put(STATUS, ERROR).put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, filterPatternResult.getErr()));
        } else {
            redisClient.zrangebyscore(getQueuesKey(), String.valueOf(getMaxAgeTimestamp()), "+inf",
                    RangeLimitOptions.NONE, new GetQueuesHandler(event, filterPatternResult.getOk(), countOnly));
        }
    }

    private void getQueuesCount(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        if (result.isErr()) {
            event.reply(new JsonObject().put(STATUS, ERROR).put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, result.getErr()));
            return;
        }

        /*
         * to filter values we have to use "getQueues" operation
         */
        if (result.getOk().isPresent()) {
            getQueues(event, true, result);
        } else {
            redisClient.zcount(getQueuesKey(), getMaxAgeTimestamp(), Double.MAX_VALUE, new GetQueuesCountHandler(event));
        }
    }

    private void getQueueItem(Message<JsonObject> event) {
        String key = getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int index = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        redisClient.lindex(key, index, new GetQueueItemHandler(event));
    }

    private void replaceQueueItem(Message<JsonObject> event) {
        String keyReplaceItem = getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexReplaceItem = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        String bufferReplaceItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        redisClient.lset(keyReplaceItem, indexReplaceItem, bufferReplaceItem, new ReplaceQueueItemHandler(event));
    }

    private void deleteQueueItem(Message<JsonObject> event) {
        String keyLset = getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexLset = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        redisClient.lset(keyLset, indexLset, "TO_DELETE", event1 -> {
            if (event1.succeeded()) {
                String keyLrem = getQueuesPrefix() + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
                redisClient.lrem(keyLrem, 0, "TO_DELETE", replyLrem -> {
                    event.reply(new JsonObject().put(STATUS, OK));
                });
            } else {
                event.reply(new JsonObject().put(STATUS, ERROR));
            }
        });
    }

    private void deleteAllQueueItems(Message<JsonObject> event) {
        JsonObject payload = event.body().getJsonObject(PAYLOAD);
        boolean unlock = payload.getBoolean(UNLOCK, false);
        String queue = payload.getString(QUEUENAME);
        redisClient.del(buildQueueKey(queue), deleteReply -> {
            if (unlock) {
                redisClient.hdel(getLocksKey(), queue, unlockReply -> replyResultGreaterThanZero(event, deleteReply));
            } else {
                replyResultGreaterThanZero(event, deleteReply);
            }
        });
    }

    private void getQueueProcessMessageFailureCount(String queue, Handler<AsyncResult<Integer>> handler) {
        redisClient.get(getQueueProcessMessageFailureCountPrefix() + queue, failureCountResult -> {
            int failureCount;
            if (failureCountResult.result() != null) {
                failureCount = Integer.parseInt(failureCountResult.result());
            } else {
                failureCount = 0;
            }
            
            handler.handle(Future.succeededFuture(failureCount));
        });
    }
    
    void updateQueueProcessMessageFailureCount(String queue, boolean isProcessMessageSuccessful, Handler<AsyncResult<Void>> handler) {
        if (isProcessMessageSuccessful) {
            redisClient.set(getQueueProcessMessageFailureCountPrefix() + queue, "0", handler);
        } else {
            getQueueProcessMessageFailureCount(queue, asyncResult -> {
                redisClient.set(getQueueProcessMessageFailureCountPrefix() + queue, String.valueOf(asyncResult.result() + 1), handler);
            });
        }
    }
    
    void getQueueRescheduleRefreshPeriod(String queue, Handler<AsyncResult<Integer>> handler) {
        getQueueProcessMessageFailureCount(queue, failureCountAsyncResult -> {
            int failureCount = failureCountAsyncResult.result();

            int rescheduleRefreshPeriod;
            int slowDown = refreshPeriod + (failureCount * slowDownExtension);
            rescheduleRefreshPeriod = slowDown <= maxSlowDown ? slowDown : maxSlowDown;

            handler.handle(Future.succeededFuture(rescheduleRefreshPeriod));
        });
    }
    
    private String buildQueueKey(String queue){
        return getQueuesPrefix() + queue;
    }

    private List<String> buildQueueKeys(JsonArray queues) {
        if(queues == null){
            return null;
        }
        List<String> queueKeys = new ArrayList<>();
        for (int i = 0; i < queues.size(); i++) {
            String queue = queues.getString(i);
            queueKeys.add(buildQueueKey(queue));
        }
        return queueKeys;
    }

    private void bulkDeleteQueues(Message<JsonObject> event){
        JsonArray queues = event.body().getJsonObject(PAYLOAD).getJsonArray(QUEUES);
        if (queues == null) {
            event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, "No queues to delete provided"));
            return;
        }

        if(queues.isEmpty()){
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, 0));
            return;
        }

        if(!jsonArrayContainsStringsOnly(queues)){
            event.reply(new JsonObject().put(STATUS, ERROR).put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Queues must be string values"));
            return;
        }

        redisClient.delMany(buildQueueKeys(queues), delManyReply -> {
            if(delManyReply.succeeded()){
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, delManyReply.result()));
            } else {
                event.reply(new JsonObject().put(STATUS, ERROR));
            }
        });
    }

    private void replyResultGreaterThanZero(Message<JsonObject> event, AsyncResult<Long> reply) {
        if (reply.succeeded() && reply.result() != null && reply.result() > 0) {
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, reply.result()));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }

    private void getAllLocks(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        if (result.isOk()) {
            redisClient.hkeys(getLocksKey(), new GetAllLocksHandler(event, result.getOk()));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR).put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, result.getErr()));
        }
    }

    private void putLock(Message<JsonObject> event) {
        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo != null) {
            JsonArray lockNames = new JsonArray().add(event.body().getJsonObject(PAYLOAD).getString(QUEUENAME));
            if(!jsonArrayContainsStringsOnly(lockNames)){
                event.reply(new JsonObject().put(STATUS, ERROR).put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Lock must be a string value"));
                return;
            }
            redisClient.hmset(getLocksKey(), buildLocksItems(lockNames, lockInfo), new PutLockHandler(event));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
        }
    }

    private void bulkPutLocks(Message<JsonObject> event) {
        JsonArray locks = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (locks == null || locks.isEmpty()) {
            event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, "No locks to put provided"));
            return;
        }

        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo == null) {
            event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
            return;
        }

        if(!jsonArrayContainsStringsOnly(locks)){
            event.reply(new JsonObject().put(STATUS, ERROR).put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Locks must be string values"));
            return;
        }

        redisClient.hmset(getLocksKey(), buildLocksItems(locks, lockInfo), new PutLockHandler(event));
    }

    private JsonObject buildLocksItems(JsonArray lockNames, JsonObject lockInfo) {
        JsonObject obj = new JsonObject();
        String lockInfoStr = lockInfo.encode();
        for (int i = 0; i < lockNames.size(); i++) {
            String lock = lockNames.getString(i);
            obj.put(lock, lockInfoStr);
        }
        return obj;
    }

    private void deleteLock(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        redisClient.exists(getQueuesPrefix() + queueName, event1 -> {
            if (event1.succeeded() && event1.result() != null && event1.result() == 1) {
                notifyConsumer(queueName);
            }
            redisClient.hdel(getLocksKey(), queueName, new DeleteLockHandler(event));
        });
    }

    private void bulkDeleteLocks(Message<JsonObject> event) {
        JsonArray locks = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (locks != null) {
            deleteLocks(event, locks);
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, "No locks to delete provided"));
        }
    }

    private void deleteAllLocks(Message<JsonObject> event) {
        redisClient.hkeys(getLocksKey(), locksResult -> {
            if (locksResult.succeeded()) {
                JsonArray locks = locksResult.result();
                deleteLocks(event, locks);
            } else {
                log.warn("failed to delete all locks. Message: " + locksResult.cause().getMessage());
                event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, locksResult.cause().getMessage()));
            }
        });
    }

    private void deleteLocks(Message<JsonObject> event, JsonArray locks) {
        if (locks == null || locks.isEmpty()) {
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, 0));
            return;
        }

        if(!jsonArrayContainsStringsOnly(locks)){
            event.reply(new JsonObject().put(STATUS, ERROR).put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Locks must be string values"));
            return;
        }

        redisClient.hdelMany(getLocksKey(), locks.getList(), delManyResult -> {
            if (delManyResult.succeeded()) {
                log.info("Successfully deleted " + delManyResult.result() + " locks");
                event.reply(new JsonObject().put(STATUS, OK).put(VALUE, delManyResult.result()));
            } else {
                log.warn("failed to delete locks. Message: " + delManyResult.cause().getMessage());
                event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, delManyResult.cause().getMessage()));
            }
        });
    }

    private boolean jsonArrayContainsStringsOnly(JsonArray array){
        try {
            for (int i = 0; i < array.size(); i++) {
                array.getString(i);
            }
            return true;
        } catch(Exception ex){
            return false;
        }
    }

    private void getConfiguration(Message<JsonObject> event) {
        JsonObject result = new JsonObject();
        result.put(RedisquesConfiguration.PROP_ADDRESS, address);
        result.put(RedisquesConfiguration.PROP_REDIS_PREFIX, redisPrefix);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_ADDRESS, processorAddress);
        result.put(RedisquesConfiguration.PROP_REFRESH_PERIOD, refreshPeriod);
        result.put(RedisquesConfiguration.PROP_REDIS_HOST, redisHost);
        result.put(RedisquesConfiguration.PROP_REDIS_PORT, redisPort);
        result.put(RedisquesConfiguration.PROP_REDIS_AUTH, redisAuth);
        result.put(RedisquesConfiguration.PROP_REDIS_ENCODING, redisEncoding);
        result.put(RedisquesConfiguration.PROP_CHECK_INTERVAL, checkInterval);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_TIMEOUT, processorTimeout);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_DELAY_MAX, processorDelayMax);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_ENABLED, httpRequestHandlerEnabled);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_PREFIX, httpRequestHandlerPrefix);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_PORT, httpRequestHandlerPort);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_USER_HEADER, httpRequestHandlerUserHeader);
        event.reply(new JsonObject().put(STATUS, OK).put(VALUE, result));
    }

    private void setConfiguration(Message<JsonObject> event) {
        JsonObject configurationValues = event.body().getJsonObject(PAYLOAD);
        setConfigurationValues(configurationValues, true).setHandler(setConfigurationValuesEvent -> {
            if (setConfigurationValuesEvent.succeeded()) {
                log.debug("About to publish the configuration updates to event bus address '" + configurationUpdatedAddress + "'");
                vertx.eventBus().publish(configurationUpdatedAddress, configurationValues);
                event.reply(setConfigurationValuesEvent.result());
            } else {
                event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, setConfigurationValuesEvent.cause().getMessage()));
            }
        });
    }

    private Future<JsonObject> setConfigurationValues(JsonObject configurationValues, boolean validateOnly) {
        Future<JsonObject> future = Future.future();

        if (configurationValues != null) {
            List<String> notAllowedConfigurationValues = findNotAllowedConfigurationValues(configurationValues.fieldNames());
            if (notAllowedConfigurationValues.isEmpty()) {
                try {
                    Long processorDelayMaxValue = configurationValues.getLong(PROCESSOR_DELAY_MAX);
                    if (processorDelayMaxValue == null) {
                        future.fail("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is missing");
                        return future;
                    }
                    if (!validateOnly) {
                        this.processorDelayMax = processorDelayMaxValue;
                        log.info("Updated configuration value of property '" + PROCESSOR_DELAY_MAX + "' to " + processorDelayMaxValue);
                    }
                    future.complete(new JsonObject().put(STATUS, OK));
                } catch (ClassCastException ex) {
                    future.fail("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is not a number");
                }
            } else {
                String notAllowedConfigurationValuesString = notAllowedConfigurationValues.toString();
                future.fail("Not supported configuration values received: " + notAllowedConfigurationValuesString);
            }
        } else {
            future.fail("Configuration values missing");
        }

        return future;
    }

    private List<String> findNotAllowedConfigurationValues(Set<String> configurationValues) {
        if (configurationValues == null) {
            return Collections.emptyList();
        }
        return configurationValues.stream().filter(p -> !ALLOWED_CONFIGURATION_VALUES.contains(p)).collect(Collectors.toList());
    }

    private void registerQueueCheck(RedisquesConfiguration modConfig) {
        vertx.setPeriodic(modConfig.getCheckIntervalTimerMs(), periodicEvent -> luaScriptManager.handleQueueCheck(getQueueCheckLastexecKey(),
                checkInterval, shouldCheck -> {
                    if (shouldCheck) {
                        log.info("periodic queue check is triggered now");
                        checkQueues();
                    }
                }));
    }

    private long getMaxAgeTimestamp() {
        return System.currentTimeMillis() - MAX_AGE_MILLISECONDS;
    }

    private void unsupportedOperation(String operation, Message<JsonObject> event) {
        JsonObject reply = new JsonObject();
        String message = "QUEUE_ERROR: Unsupported operation received: " + operation;
        log.error(message);
        reply.put(STATUS, ERROR);
        reply.put(MESSAGE, message);
        event.reply(reply);
    }

    private JsonObject extractLockInfo(String requestedBy) {
        if (requestedBy == null) {
            return null;
        }
        JsonObject lockInfo = new JsonObject();
        lockInfo.put(REQUESTED_BY, requestedBy);
        lockInfo.put(TIMESTAMP, System.currentTimeMillis());
        return lockInfo;
    }

    @Override
    public void stop() {
        unregisterConsumers(true);
    }

    private void gracefulStop(final Handler<Void> doneHandler) {
        conumersMessageConsumer.unregister(event -> uidMessageConsumer.unregister(event1 -> {
            unregisterConsumers(false);
            stoppedHandler = doneHandler;
            if (myQueues.keySet().isEmpty()) {
                doneHandler.handle(null);
            }
        }));
    }

    private void unregisterConsumers(boolean force) {
        if (log.isTraceEnabled()) {
            log.trace("RedisQues unregister consumers force: " + force);
        }
        log.debug("RedisQues Unregistering consumers");
        for (final Map.Entry<String, QueueState> entry : myQueues.entrySet()) {
            final String queue = entry.getKey();
            if (force || entry.getValue() == QueueState.READY) {
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues unregister consumers queue: " + queue);
                }
                refreshRegistration(queue, event -> {
                    // Make sure that I am still the registered consumer
                    String consumerKey = getConsumersPrefix() + queue;
                    if (log.isTraceEnabled()) {
                        log.trace("RedisQues unregister consumers get: " + consumerKey);
                    }
                    redisClient.get(consumerKey, event1 -> {
                        String consumer = event1.result();
                        if (log.isTraceEnabled()) {
                            log.trace("RedisQues unregister consumers get result: " + consumer);
                        }
                        if (uid.equals(consumer)) {
                            log.debug("RedisQues remove consumer: " + uid);
                            myQueues.remove(queue);
                        }
                    });
                });
            }
        }
    }

    /**
     * Caution: this may in some corner case violate the ordering for one
     * message.
     */
    private void resetConsumers() {
        log.debug("RedisQues Resetting consumers");
        String keysPattern = getConsumersPrefix() + "*";
        if (log.isTraceEnabled()) {
            log.trace("RedisQues reset consumers keys: " + keysPattern);
        }
        redisClient.keys(keysPattern, keysResult -> {
            if (keysResult.failed() || keysResult.result() == null) {
                log.error("Unable to get redis keys of consumers", keysResult.cause());
                return;
            }
            JsonArray keys = keysResult.result();
            if (keys == null || keys.isEmpty()) {
                log.debug("No consumers found to reset");
                return;
            }
            redisClient.delMany(keys.getList(), delManyResult -> {
                if (delManyResult.succeeded()) {
                    Long count = delManyResult.result();
                    log.debug("Successfully reset " + count + " consumers");
                } else {
                    log.error("Unable to delete redis keys of consumers");
                }
            });
        });
    }

    private void consume(final String queue) {
        log.debug(" RedisQues Requested to consume queue " + queue);
        refreshRegistration(queue, event -> {
            // Make sure that I am still the registered consumer
            String key = getConsumersPrefix() + queue;
            if (log.isTraceEnabled()) {
                log.trace("RedisQues consume get: " + key);
            }
            redisClient.get(key, event1 -> {
                if (event1.failed()) {
                    log.error("Unable to get consumer for queue " + queue, event1.cause());
                    return;
                }
                String consumer = event1.result();
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues refresh registration consumer: " + consumer);
                }
                if (uid.equals(consumer)) {
                    QueueState state = myQueues.get(queue);
                    if (log.isTraceEnabled()) {
                        log.trace("RedisQues consumer: " + consumer + " queue: " + queue + " state: " + state);
                    }
                    // Get the next message only once the previous has
                    // been completely processed
                    if (state != QueueState.CONSUMING) {
                        myQueues.put(queue, QueueState.CONSUMING);
                        if (state == null) {
                            // No previous state was stored. Maybe the
                            // consumer was restarted
                            log.warn("Received request to consume from a queue I did not know about: " + queue);
                        }
                        log.debug("RedisQues Starting to consume queue " + queue);
                        readQueue(queue);

                    } else {
                        log.debug("RedisQues Queue " + queue + " is already beeing consumed");
                    }
                } else {
                    // Somehow registration changed. Let's renotify.
                    log.warn("Registration for queue " + queue + " has changed to " + consumer);
                    myQueues.remove(queue);
                    notifyConsumer(queue);
                }
            });
        });
    }

    private Future<Boolean> isQueueLocked(final String queue) {
        Future<Boolean> future = Future.future();
        redisClient.hexists(getLocksKey(), queue, event -> {
            if (event.failed() || event.result() == null) {
                log.warn("failed to check if queue '" + queue + "' is locked. Message: " + event.cause().getMessage());
                future.complete(Boolean.FALSE);
            } else {
                future.complete(event.result() == 1);
            }
        });
        return future;
    }

    private void readQueue(final String queue) {
        if (log.isTraceEnabled()) {
            log.trace("RedisQues read queue: " + queue);
        }
        String key = getQueuesPrefix() + queue;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues read queue lindex: " + key);
        }

        isQueueLocked(queue).setHandler(lockAnswer -> {
            boolean locked = lockAnswer.result();
            if (!locked) {
                redisClient.lindex(key, 0, answer -> {
                    if (log.isTraceEnabled()) {
                        log.trace("RedisQues read queue lindex result: " + answer.result());
                    }
                    if (answer.result() != null) {
                        processMessageWithTimeout(queue, answer.result(), sendResult -> {
                            if (sendResult.success) {
                                // Remove the processed message from the
                                // queue
                                String key1 = getQueuesPrefix() + queue;
                                if (log.isTraceEnabled()) {
                                    log.trace("RedisQues read queue lpop: " + key1);
                                }
                                redisClient.lpop(key1, jsonAnswer -> {
                                    log.debug("RedisQues Message removed, queue " + queue + " is ready again");
                                    myQueues.put(queue, QueueState.READY);
                                    vertx.cancelTimer(sendResult.timeoutId);
                                    // Notify that we are stopped in
                                    // case it
                                    // was the last active consumer
                                    if (stoppedHandler != null) {
                                        unregisterConsumers(false);
                                        if (myQueues.isEmpty()) {
                                            stoppedHandler.handle(null);
                                        }
                                    }
                                    // Issue notification to consume next message if any
                                    String key2 = getQueuesPrefix() + queue;
                                    if (log.isTraceEnabled()) {
                                        log.trace("RedisQues read queue: " + key);
                                    }
                                    redisClient.llen(key2, answer1 -> {
                                        if (answer1.succeeded() && answer1.result() != null && answer1.result() > 0) {
                                            notifyConsumer(queue);
                                        }
                                    });
                                });
                            } else {
                                // Failed. Message will be kept in queue and retried later
                                log.debug("RedisQues Processing failed for queue " + queue);
                                
                                // reschedule
                                getQueueRescheduleRefreshPeriod(queue, rescheduleRefreshPeriodAsyncResult -> {
                                    int rescheduleRefreshPeriod = rescheduleRefreshPeriodAsyncResult.result();
                                    log.warn("RedisQues will re-send the message to queue '" + queue + "' in " + rescheduleRefreshPeriod + " seconds");
                                    vertx.cancelTimer(sendResult.timeoutId);
                                    rescheduleSendMessageAfterFailure(queue, rescheduleRefreshPeriod);
                                });
                            }
                        });
                    } else {
                        // This can happen when requests to consume happen at the same moment the queue is emptied.
                        log.debug("Got a request to consume from empty queue " + queue);
                        myQueues.put(queue, QueueState.READY);
                    }
                });
            } else {
                log.debug("Got a request to consume from locked queue " + queue);
                myQueues.put(queue, QueueState.READY);
            }
        });
    }

    private void rescheduleSendMessageAfterFailure(final String queue, int refreshPeriod) {
        if (log.isTraceEnabled()) {
            log.trace("RedsQues reschedule after failure for queue: " + queue);
        }
        
        vertx.setTimer(refreshPeriod * 1000, timerId -> {
            log.warn("RedisQues re-notify the consumer of queue '" + queue + "' at " + new Date(System.currentTimeMillis()));
            notifyConsumer(queue);

            // reset the queue state to be consumed by {@link RedisQues#consume(String)}
            myQueues.put(queue, QueueState.READY);
        });
    }

    private void processMessageWithTimeout(final String queue, final String payload, final Handler<SendResult> handler) {
        if (processorDelayMax > 0) {
            log.info("About to process message for queue " + queue + " with a maximum delay of " + processorDelayMax + "ms");
        }
        timer.executeDelayedMax(processorDelayMax).setHandler(delayed -> {
            if (delayed.failed()) {
                log.error("Delayed execution has failed. Cause: " + delayed.cause().getMessage());
                return;
            }
            final EventBus eb = vertx.eventBus();
            JsonObject message = new JsonObject();
            message.put("queue", queue);
            message.put(PAYLOAD, payload);
            if (log.isTraceEnabled()) {
                log.trace("RedisQues process message: " + message + " for queue: " + queue + " send it to processor: " + processorAddress);
            }

            // start a timer, which will cancel the processing, if the consumer didn't respond
            final long timeoutId = vertx.setTimer(processorTimeout, timeoutId1 -> {
                log.info("RedisQues QUEUE_ERROR: Consumer timeout " + uid + " queue: " + queue);
                handler.handle(new SendResult(false, timeoutId1));
            });

            // send the message to the consumer
            eb.send(processorAddress, message, (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                Boolean success;
                if (reply.succeeded()) {
                    success = OK.equals(reply.result().body().getString(STATUS));
                } else {
                    success = Boolean.FALSE;
                }
                
                // update the queue failure count
                updateQueueProcessMessageFailureCount(queue, success, asyncResult -> 
                        // send the result
                        handler.handle(new SendResult(success, timeoutId)));
            });
            updateTimestamp(queue, null);
        });
    }

    private class SendResult {
        public final Boolean success;
        public final Long timeoutId;

        public SendResult(Boolean success, Long timeoutId) {
            this.success = success;
            this.timeoutId = timeoutId;
        }
    }

    private void notifyConsumer(final String queue) {
        log.debug("RedisQues Notifying consumer of queue " + queue);
        final EventBus eb = vertx.eventBus();

        // Find the consumer to notify
        String key = getConsumersPrefix() + queue;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues notify consumer get: " + key);
        }
        redisClient.get(key, event -> {
            String consumer = event.result();
            if (log.isTraceEnabled()) {
                log.trace("RedisQues got consumer: " + consumer);
            }
            if (consumer == null) {
                // No consumer for this queue, let's make a peer become
                // consumer
                log.debug("RedisQues Sending registration request for queue " + queue);
                eb.send(address + "-consumers", queue);
            } else {
                // Notify the registered consumer
                log.debug("RedisQues Notifying consumer " + consumer + " to consume queue " + queue);
                eb.send(consumer, queue);
            }
        });
    }

    private void refreshRegistration(String queue, Handler<AsyncResult<Long>> handler) {
        log.debug("RedisQues Refreshing registration of queue " + queue + ", expire in " + consumerLockTime + " s");
        String key = getConsumersPrefix() + queue;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues refresh registration: " + key);
        }
        if (handler != null) {
            redisClient.expire(key, consumerLockTime, handler);
        } else {
            redisClient.expire(key, consumerLockTime, event -> {
            });
        }
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queue   the name of the queue
     * @param handler (optional) To get informed when done.
     */
    private void updateTimestamp(final String queue, Handler<AsyncResult<Long>> handler) {
        long ts = System.currentTimeMillis();
        if (log.isTraceEnabled()) {
            log.trace("RedisQues update timestamp for queue: " + queue + " to: " + ts);
        }
        if (handler != null) {
            redisClient.zadd(getQueuesKey(), ts, queue, handler);
        } else {
            redisClient.zadd(getQueuesKey(), ts, queue, event -> {
            });
        }
    }

    /**
     * Notify not-active/not-empty queues to be processed (e.g. after a reboot).
     * Check timestamps of not-active/empty queues.
     * This uses a sorted set of queue names scored by last update timestamp.
     */
    private void checkQueues() {
        log.debug("Checking queues timestamps");
        // List all queues that look inactive (i.e. that have not been updated since 3 periods).
        final long limit = System.currentTimeMillis() - 3 * refreshPeriod * 1000;
        redisClient.zrangebyscore(getQueuesKey(), "-inf", String.valueOf(limit), RangeLimitOptions.NONE, answer -> {
            JsonArray queues = answer.result();
            if (answer.failed() || queues == null) {
                log.error("RedisQues is unable to get list of queues", answer.cause());
                return;
            }
            final AtomicInteger counter = new AtomicInteger(queues.size());
            if (log.isTraceEnabled()) {
                log.trace("RedisQues update queues: " + counter);
            }
            for (Object queueObject : queues) {
                // Check if the inactive queue is not empty (i.e. the key exists)
                final String queue = (String) queueObject;
                String key = getQueuesPrefix() + queue;
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues update queue: " + key);
                }
                redisClient.exists(key, event -> {
                    if (event.failed() || event.result() == null) {
                        log.error("RedisQues is unable to check existence of queue " + queue, event.cause());
                        return;
                    }
                    if (event.result() == 1) {
                        log.debug("Updating queue timestamp " + queue);
                        // If not empty, update the queue timestamp to keep it in the sorted set.
                        updateTimestamp(queue, message -> {
                            // Ensure we clean the old queues after having updated all timestamps
                            if (counter.decrementAndGet() == 0) {
                                removeOldQueues(limit);
                            }
                        });
                        // Make sure its TTL is correctly set (replaces the previous orphan detection mechanism).
                        refreshRegistration(queue, null);
                        // And trigger its consumer.
                        notifyConsumer(queue);
                    } else {
                        // Ensure we clean the old queues also in the case of empty queue.
                        if (log.isTraceEnabled()) {
                            log.trace("RedisQues remove old queue: " + queue);
                        }
                        if (counter.decrementAndGet() == 0) {
                            removeOldQueues(limit);
                        }
                    }
                });
            }
        });
    }

    /**
     * Remove queues from the sorted set that are timestamped before a limit time.
     *
     * @param limit limit timestamp
     */
    private void removeOldQueues(long limit) {
        log.debug("Cleaning old queues");
        redisClient.zremrangebyscore(getQueuesKey(), "-inf", String.valueOf(limit), event -> {
        });
    }

    private int getMaxQueueItemCountIndex(String limit) {
        int defaultMaxIndex = DEFAULT_MAX_QUEUEITEM_COUNT;
        if (limit != null) {
            try {
                int maxIndex = Integer.parseInt(limit) - 1;
                if (maxIndex >= 0) {
                    defaultMaxIndex = maxIndex;
                }
                log.info("use limit parameter " + maxIndex);
            } catch (NumberFormatException ex) {
                log.warn("Invalid limit parameter '" + limit + "' configured for max queue item count. Using default " + DEFAULT_MAX_QUEUEITEM_COUNT);
            }
        }
        return defaultMaxIndex;
    }
}
