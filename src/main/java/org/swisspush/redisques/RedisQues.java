package org.swisspush.redisques;

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.*;
import io.vertx.redis.client.impl.types.MultiType;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.handler.*;
import org.swisspush.redisques.lua.LuaScriptManager;
import org.swisspush.redisques.util.*;

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
    private final String uid = UUID.randomUUID().toString();

    private MessageConsumer<String> uidMessageConsumer;

    // The queues this verticle is listening to
    private final Map<String, QueueState> myQueues = new HashMap<>();

    private final Logger log = LoggerFactory.getLogger(RedisQues.class);

    private QueueStatisticsCollector queueStatisticsCollector;

    private Handler<Void> stoppedHandler = null;

    private MessageConsumer<String> consumersMessageConsumer;

    // Configuration

    // Address of this redisques. Also used as prefix for consumer broadcast
    // address.
    private String address = "redisques";

    private String configurationUpdatedAddress = "redisques-configuration-updated";

    private RedisAPI redisAPI;

    // Prefix for redis keys holding queues and consumers.
    private String redisPrefix;

    // varia more specific prefixes
    private String queuesKey;
    private String queuesPrefix;
    private String consumersPrefix;
    private String locksKey;
    private String queueCheckLastexecKey;

    // Address of message processors
    private String processorAddress = "redisques-processor";

    public static final String TIMESTAMP = "timestamp";

    // Consumers periodically refresh their subscription while they are consuming
    private int refreshPeriod;
    private int consumerLockTime;

    private int checkInterval;

    // the time we wait for the processor to answer, before we cancel processing
    private int processorTimeout = 240000;

    private long processorDelayMax;
    private RedisQuesTimer timer;

    private String redisHost;
    private int redisPort;
    private String redisAuth;
    private int redisMaxPoolSize;
    private int redisMaxPoolWaitingSize;
    private int redisMaxPipelineWaitingSize;
    private int memoryUsageLimitPct;
    private int queueSpeedIntervalSec;
    private boolean enableQueueNameDecoding;
    private boolean httpRequestHandlerEnabled;
    private String httpRequestHandlerPrefix;
    private int httpRequestHandlerPort;
    private String httpRequestHandlerUserHeader;
    private List<QueueConfiguration> queueConfigurations;

    private static final int DEFAULT_MAX_QUEUEITEM_COUNT = 49;
    private static final int MAX_AGE_MILLISECONDS = 120000; // 120 seconds

    private static final Set<String> ALLOWED_CONFIGURATION_VALUES = Stream.of("processorDelayMax")
            .collect(Collectors.toSet());

    private LuaScriptManager luaScriptManager;


    private void redisSetWithOptions(String key, String value, boolean nx, int ex,
                                     Handler<AsyncResult<Response>> handler) {
        JsonArray options = new JsonArray();
        options.add("EX").add(ex);
        if (nx) {
            options.add("NX");
        }
        redisAPI.send(Command.SET, RedisUtils.toPayload(key, value, options).toArray(new String[0]))
                .onComplete(handler);
    }

    /**
     * <p>Handler receiving registration requests when no consumer is registered
     * for a queue.</p>
     */
    private void handleRegistrationRequest(Message<String> msg) {
        final String queueName = msg.body();
        if (queueName == null) {
            log.warn("Got message without queue name while handleRegistrationRequest.");
            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
        }
        if (log.isDebugEnabled()) {
            log.debug(
                    "RedisQues Got registration request for queue {} from consumer: {}", queueName, uid);
        }
        // Try to register for this queue
        redisSetWithOptions(consumersPrefix + queueName, uid, true, consumerLockTime, event -> {
            if (event.succeeded()) {
                String value = event.result() != null ? event.result().toString() : null;
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues setxn result: " + value + " for queue: " + queueName);
                }
                if ("OK".equals(value)) {
                    // I am now the registered consumer for this queue.
                    if (log.isDebugEnabled()) {
                        log.debug("RedisQues Now registered for queue " + queueName);
                    }
                    myQueues.put(queueName, QueueState.READY);
                    consume(queueName);
                } else {
                    log.debug("RedisQues Missed registration for queue " + queueName);
                    // Someone else just became the registered consumer. I
                    // give up.
                }
            } else {
                log.error("redisSetWithOptions failed", event.cause());
            }
        });
    }

    @Override
    public void start(Promise<Void> promise) {
        final EventBus eb = vertx.eventBus();
        log.info("Started with UID " + uid);

        RedisquesConfiguration modConfig = RedisquesConfiguration.fromJsonObject(config());
        log.info("Starting Redisques module with configuration: " + modConfig);

        address = modConfig.getAddress();
        configurationUpdatedAddress = modConfig.getConfigurationUpdatedAddress();
        redisPrefix = modConfig.getRedisPrefix(); // default: "redisques:"
        queuesKey = redisPrefix + "queues";
        queuesPrefix = redisPrefix + "queues:";
        consumersPrefix = redisPrefix + "consumers:";
        locksKey = redisPrefix + "locks";
        queueCheckLastexecKey = redisPrefix + "check:lastexec";
        processorAddress = modConfig.getProcessorAddress();
        refreshPeriod = modConfig.getRefreshPeriod();
        consumerLockTime = 2 * refreshPeriod; // lock is kept twice as long as its refresh interval -> never expires as long as the consumer ('we') are alive
        checkInterval = modConfig.getCheckInterval();
        processorTimeout = modConfig.getProcessorTimeout();
        processorDelayMax = modConfig.getProcessorDelayMax();
        memoryUsageLimitPct = modConfig.getMemoryUsageLimitPct();
        queueSpeedIntervalSec = modConfig.getQueueSpeedIntervalSec();
        enableQueueNameDecoding = modConfig.getEnableQueueNameDecoding();
        timer = new RedisQuesTimer(vertx);

        redisHost = modConfig.getRedisHost();
        redisPort = modConfig.getRedisPort();
        redisAuth = modConfig.getRedisAuth();
        redisMaxPoolSize = modConfig.getMaxPoolSize();
        redisMaxPoolWaitingSize = modConfig.getMaxPoolWaitSize();
        redisMaxPipelineWaitingSize = modConfig.getMaxPipelineWaitSize();

        httpRequestHandlerEnabled = modConfig.getHttpRequestHandlerEnabled();
        httpRequestHandlerPrefix = modConfig.getHttpRequestHandlerPrefix();
        httpRequestHandlerPort = modConfig.getHttpRequestHandlerPort();
        httpRequestHandlerUserHeader = modConfig.getHttpRequestHandlerUserHeader();
        queueConfigurations = modConfig.getQueueConfigurations();

        setupRedisAPI(redisHost, redisPort, redisAuth, redisMaxPoolSize, redisMaxPoolWaitingSize, redisMaxPipelineWaitingSize).onComplete(event -> {
            if(event.succeeded()){
                redisAPI = event.result();
                initialize(modConfig);
                promise.complete();
            } else {
                promise.fail(event.cause());
            }
        });
    }

    private void initialize(RedisquesConfiguration modConfig) {
        this.luaScriptManager = new LuaScriptManager(redisAPI);
        this.queueStatisticsCollector = new QueueStatisticsCollector(redisAPI, luaScriptManager,
                queuesPrefix, vertx, modConfig.getQueueSpeedIntervalSec());

        RedisquesHttpRequestHandler.init(vertx, modConfig);

        vertx.eventBus().consumer(configurationUpdatedAddress, (Handler<Message<JsonObject>>) event -> {
            log.info("Received configurations update");
            setConfigurationValues(event.body(), false);
        });

        // Handles operations
        vertx.eventBus().consumer(address, operationsHandler());

        // Handles registration requests
        consumersMessageConsumer = vertx.eventBus().consumer(address + "-consumers", this::handleRegistrationRequest);

        // Handles notifications
        uidMessageConsumer = vertx.eventBus().consumer(uid, event -> {
            final String queue = event.body();
            if (queue == null) {
                log.warn("Got event bus msg with empty body! uid={}  address={}  replyAddress={}", uid, event.address(), event.replyAddress());
                // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
            }
            log.debug("RedisQues got notification for queue '{}'", queue);
            consume(queue);
        });

        registerActiveQueueRegistrationRefresh();
        registerQueueCheck(modConfig);
    }

    private Future<RedisAPI> setupRedisAPI(String redisHost, Integer redisPort, String redisAuth,
                                            int redisMaxPoolSize, int redisMaxPoolWaitingSize, int redisMaxPipelineWaitingSize) {
        Promise<RedisAPI> promise = Promise.promise();
        Redis.createClient(vertx, new RedisOptions()
                .setConnectionString("redis://" + redisHost + ":" + redisPort)
                .setPassword((redisAuth == null ? "" : redisAuth))
                .setMaxPoolSize(redisMaxPoolSize)
                .setMaxPoolWaiting(redisMaxPoolWaitingSize)
                .setMaxWaitingHandlers(redisMaxPipelineWaitingSize)
        ).connect(event -> {
            if (event.failed()) {
                promise.fail(event.cause());
            } else {
                promise.complete(RedisAPI.api(event.result()));
            }
        });

        return promise.future();
    }

    private void registerActiveQueueRegistrationRefresh() {
        // Periodic refresh of my registrations on active queues.
        vertx.setPeriodic(refreshPeriod * 1000, event -> {
            // Check if I am still the registered consumer
            myQueues.entrySet().stream().filter(entry -> entry.getValue() == QueueState.CONSUMING).
                    forEach(entry -> {
                        final String queue = entry.getKey();
                        // Check if I am still the registered consumer
                        String consumerKey = consumersPrefix + queue;
                        if (log.isTraceEnabled()) {
                            log.trace("RedisQues refresh queues get: " + consumerKey);
                        }
                        redisAPI.get(consumerKey, getConsumerEvent -> {
                            if (getConsumerEvent.failed()) {
                                log.warn("Failed to get queue consumer for queue '{}'. But we'll continue anyway :)", queue, getConsumerEvent.cause());
                                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                            }
                            final String consumer = Objects.toString(getConsumerEvent.result(), "");
                            if (uid.equals(consumer)) {
                                log.debug("RedisQues Periodic consumer refresh for active queue " + queue);
                                refreshRegistration(queue, null);
                                updateTimestamp(queue, null);
                            } else {
                                log.debug("RedisQues Removing queue " + queue + " from the list");
                                myQueues.remove(queue);
                                queueStatisticsCollector.resetQueueFailureStatistics(queue);
                            }
                        });
                    });
        });
    }

    private Handler<Message<JsonObject>> operationsHandler() {
        return event -> {
            final JsonObject body = event.body();
            if (null == body) {
                log.warn("Got msg with empty body from event bus. We'll run directly in a NullPointerException now. address={}  replyAddress={} ", event.address(), event.replyAddress());
                // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
            }
            String operation = body.getString(OPERATION);
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
                    break;
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
                    redisAPI.hget(locksKey, body.getJsonObject(PAYLOAD).getString(QUEUENAME), new GetLockHandler(event));
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
                    getQueueItemsCount(event);
                    break;
                case getQueuesItemsCount:
                    getQueuesItemsCount(event);
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
                case getQueuesStatistics:
                    getQueuesStatistics(event);
                    break;
                case getQueuesSpeed:
                    getQueuesSpeed(event);
                    break;
                default:
                    unsupportedOperation(operation, event);
            }
        };
    }

    private void enqueue(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        updateTimestamp(queueName, null);
        String keyEnqueue = queuesPrefix + queueName;
        String valueEnqueue = event.body().getString(MESSAGE);
        redisAPI.rpush(Arrays.asList(keyEnqueue, valueEnqueue), event2 -> {
            JsonObject reply = new JsonObject();
            if (event2.succeeded()) {
                if (log.isDebugEnabled()) {
                    log.debug("RedisQues Enqueued message into queue " + queueName);
                }
                long queueLength = event2.result().toLong();
                notifyConsumer(queueName);
                reply.put(STATUS, OK);
                reply.put(MESSAGE, "enqueued");

                // feature EN-queue slow-down (the larger the queue the longer we delay "OK" response)
                long delayReplyMillis = 0;
                QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
                if (queueConfiguration != null) {
                    float enqueueDelayFactorMillis = queueConfiguration.getEnqueueDelayFactorMillis();
                    if (enqueueDelayFactorMillis > 0f) {
                        // minus one as we need the queueLength _before_ our en-queue here
                        delayReplyMillis = (long) ((queueLength - 1) * enqueueDelayFactorMillis);
                        int max = queueConfiguration.getEnqueueMaxDelayMillis();
                        if (max > 0 && delayReplyMillis > max) {
                            delayReplyMillis = max;
                        }
                    }
                }
                if (delayReplyMillis > 0) {
                    vertx.setTimer(delayReplyMillis, timeIsUp -> event.reply(reply));
                } else {
                    event.reply(reply);
                }
                queueStatisticsCollector.setQueueBackPressureTime(queueName, delayReplyMillis);
            } else {
                String message = "RedisQues QUEUE_ERROR: Error while enqueueing message into queue " + queueName;
                log.error(message, event2.cause());
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
            redisAPI.hmset(Arrays.asList(locksKey, event.body().getJsonObject(PAYLOAD).getString(QUEUENAME), lockInfo.encode()),
                    putLockResult -> {
                        if (putLockResult.succeeded()) {
                            log.debug("RedisQues lockedEnqueue locking successful, now going to enqueue");
                            enqueue(event);
                        } else {
                            log.warn("RedisQues lockedEnqueue locking failed. Skip enqueue");
                            event.reply(createErrorReply());
                        }
                    });
        } else {
            log.warn("RedisQues lockedEnqueue failed because property '" + REQUESTED_BY + "' was missing");
            event.reply(createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
        }

    }

    private void addQueueItem(Message<JsonObject> event) {
        String key1 = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        String valueAddItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        redisAPI.rpush(Arrays.asList(key1, valueAddItem), new AddQueueItemHandler(event));
    }

    private void getQueueItems(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        String keyListRange = queuesPrefix + queueName;
        int maxQueueItemCountIndex = getMaxQueueItemCountIndex(event.body().getJsonObject(PAYLOAD).getString(LIMIT));
        redisAPI.llen(keyListRange, countReply -> {
            Long queueItemCount = countReply.result().toLong();
            if (countReply.succeeded() && queueItemCount != null) {
                redisAPI.lrange(keyListRange, "0", String.valueOf(maxQueueItemCountIndex), new GetQueueItemsHandler(event, queueItemCount));
            } else {
                log.warn("Operation getQueueItems failed. But I'll not notify my caller :)", countReply.cause());
                // IMO we should 'event.fail(countReply.cause())' here. But we don't, to keep backward compatibility.
            }
        });
    }

    private void getQueues(Message<JsonObject> event, boolean countOnly) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        getQueues(event, countOnly, result);
    }

    private void getQueues(Message<JsonObject> event, boolean countOnly, Result<Optional<Pattern>, String> filterPatternResult) {
        if (filterPatternResult.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, filterPatternResult.getErr()));
        } else {
            redisAPI.zrangebyscore(
                    Arrays.asList(queuesKey, String.valueOf(getMaxAgeTimestamp()), "+inf"),
                    new GetQueuesHandler(event, filterPatternResult.getOk(), countOnly));
        }
    }

    private void getQueuesCount(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        if (result.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, result.getErr()));
            return;
        }

        /*
         * to filter values we have to use "getQueues" operation
         */
        if (result.getOk().isPresent()) {
            getQueues(event, true, result);
        } else {
            redisAPI.zcount(queuesKey, String.valueOf(getMaxAgeTimestamp()), String.valueOf(Double.MAX_VALUE), new GetQueuesCountHandler(event));
        }
    }

    private void getQueueItem(Message<JsonObject> event) {
        String key = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int index = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        redisAPI.lindex(key, String.valueOf(index), new GetQueueItemHandler(event));
    }

    private void replaceQueueItem(Message<JsonObject> event) {
        String keyReplaceItem = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexReplaceItem = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        String bufferReplaceItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
        redisAPI.lset(keyReplaceItem, String.valueOf(indexReplaceItem), bufferReplaceItem, new ReplaceQueueItemHandler(event));
    }

    private void deleteQueueItem(Message<JsonObject> event) {
        String keyLset = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        int indexLset = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
        redisAPI.lset(keyLset, String.valueOf(indexLset), "TO_DELETE", event1 -> {
            if (event1.succeeded()) {
                String keyLrem = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
                redisAPI.lrem(keyLrem, "0", "TO_DELETE", replyLrem -> {
                    if (replyLrem.failed()) {
                        log.warn("Redis 'lrem' command failed. But will continue anyway.", replyLrem.cause());
                        // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                    }
                    event.reply(createOkReply());
                });
            } else {
                log.error("Failed to 'lset' while deleteQueueItem.", event1.cause());
                event.reply(createErrorReply());
            }
        });
    }

    private void deleteAllQueueItems(Message<JsonObject> event) {
        JsonObject payload = event.body().getJsonObject(PAYLOAD);
        boolean unlock = payload.getBoolean(UNLOCK, false);
        String queue = payload.getString(QUEUENAME);
        redisAPI.del(Collections.singletonList(buildQueueKey(queue)), deleteReply -> {
            if (deleteReply.failed()) {
                log.warn("Failed to deleteAllQueueItems. But we'll continue anyway", deleteReply.cause());
                // May we should 'fail()' here. But:
                // 1st: We don't, to keep backward compatibility
                // 2nd: We don't, to may unlock below.
            }
            queueStatisticsCollector.resetQueueFailureStatistics(queue);
            if (unlock) {
                redisAPI.hdel(Arrays.asList(locksKey, queue), unlockReply -> {
                    if (unlockReply.failed()) {
                        log.warn("Failed to unlock queue '{}'. Will continue anyway", queue, unlockReply.cause());
                        // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                    }
                    handleDeleteQueueReply(event, deleteReply);
                });
            } else {
                handleDeleteQueueReply(event, deleteReply);
            }
        });
    }

    int updateQueueFailureCountAndGetRetryInterval(final String queueName, boolean sendSuccess) {
        if (sendSuccess) {
            queueStatisticsCollector.queueMessageSuccess(queueName);
            return 0;
        } else {
            // update the failure count
            long failureCount = queueStatisticsCollector.queueMessageFailed(queueName);
            // find a retry interval from the queue configurations
            QueueConfiguration queueConfiguration = findQueueConfiguration(queueName);
            if (queueConfiguration != null) {
                int[] retryIntervals = queueConfiguration.getRetryIntervals();
                if (retryIntervals != null && retryIntervals.length > 0) {
                    int retryIntervalIndex = (int) (failureCount <= retryIntervals.length ? failureCount - 1 : retryIntervals.length - 1);
                    int retryTime = retryIntervals[retryIntervalIndex];
                    queueStatisticsCollector.setQueueSlowDownTime(queueName, retryTime);
                    return retryTime;
                }
            }
        }

        return refreshPeriod;
    }

    private String buildQueueKey(String queue) {
        return queuesPrefix + queue;
    }

    private List<String> buildQueueKeys(JsonArray queues) {
        if (queues == null) {
            return null;
        }
        final int size = queues.size();
        List<String> queueKeys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String queue = queues.getString(i);
            queueKeys.add(buildQueueKey(queue));
        }
        return queueKeys;
    }

    private void bulkDeleteQueues(Message<JsonObject> event) {
        JsonArray queues = event.body().getJsonObject(PAYLOAD).getJsonArray(QUEUES);
        if (queues == null) {
            event.reply(createErrorReply().put(MESSAGE, "No queues to delete provided"));
            return;
        }

        if (queues.isEmpty()) {
            event.reply(createOkReply().put(VALUE, 0));
            return;
        }

        if (!jsonArrayContainsStringsOnly(queues)) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Queues must be string values"));
            return;
        }

        redisAPI.del(buildQueueKeys(queues), delManyReply -> {
            queueStatisticsCollector.resetQueueStatistics(queues);
            if (delManyReply.succeeded()) {
                event.reply(createOkReply().put(VALUE, delManyReply.result().toLong()));
            } else {
                log.error("Failed to bulkDeleteQueues", delManyReply.cause());
                event.reply(createErrorReply());
            }
        });
    }

    private void handleDeleteQueueReply(Message<JsonObject> event, AsyncResult<Response> reply) {
        if (reply.succeeded()) {
            event.reply(createOkReply().put(VALUE, reply.result().toLong()));
        } else {
            log.error("Failed to replyResultGreaterThanZero", reply.cause());
            event.reply(createErrorReply());
        }
    }

    private void getAllLocks(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> result = MessageUtil.extractFilterPattern(event);
        if (result.isOk()) {
            redisAPI.hkeys(locksKey, new GetAllLocksHandler(event, result.getOk()));
        } else {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, result.getErr()));
        }
    }

    private void putLock(Message<JsonObject> event) {
        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo != null) {
            JsonArray lockNames = new JsonArray().add(event.body().getJsonObject(PAYLOAD).getString(QUEUENAME));
            if (!jsonArrayContainsStringsOnly(lockNames)) {
                event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Lock must be a string value"));
                return;
            }
            redisAPI.hmset(buildLocksItems(locksKey, lockNames, lockInfo), new PutLockHandler(event));
        } else {
            event.reply(createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
        }
    }

    private void bulkPutLocks(Message<JsonObject> event) {
        JsonArray locks = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (locks == null || locks.isEmpty()) {
            event.reply(createErrorReply().put(MESSAGE, "No locks to put provided"));
            return;
        }

        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
        if (lockInfo == null) {
            event.reply(createErrorReply().put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
            return;
        }

        if (!jsonArrayContainsStringsOnly(locks)) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT).put(MESSAGE, "Locks must be string values"));
            return;
        }

        redisAPI.hmset(buildLocksItems(locksKey, locks, lockInfo), new PutLockHandler(event));
    }

    private List<String> buildLocksItems(String locksKey, JsonArray lockNames, JsonObject lockInfo) {
        List<String> list = new ArrayList<>();
        list.add(locksKey);
        String lockInfoStr = lockInfo.encode();
        for (int i = 0; i < lockNames.size(); i++) {
            String lock = lockNames.getString(i);
            list.add(lock);
            list.add(lockInfoStr);
        }
        return list;
    }

    private void deleteLock(Message<JsonObject> event) {
        String queueName = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        redisAPI.exists(Collections.singletonList(queuesPrefix + queueName), event1 -> {
            if (event1.succeeded() && event1.result() != null && event1.result().toInteger() == 1) {
                notifyConsumer(queueName);
            }
            redisAPI.hdel(Arrays.asList(locksKey, queueName), new DeleteLockHandler(event));
        });
    }

    private void bulkDeleteLocks(Message<JsonObject> event) {
        JsonArray jsonArray = event.body().getJsonObject(PAYLOAD).getJsonArray(LOCKS);
        if (jsonArray != null) {
            MultiType locks = MultiType.create(jsonArray.size(), false);
            for (int j = 0; j < jsonArray.size(); j++) {
                Response response = SimpleStringType.create(jsonArray.getString(j));
                locks.add(response);
            }
            deleteLocks(event, locks);
        } else {
            event.reply(createErrorReply().put(MESSAGE, "No locks to delete provided"));
        }
    }

    private void deleteAllLocks(Message<JsonObject> event) {
        redisAPI.hkeys(locksKey, locksResult -> {
            if (locksResult.succeeded()) {
                Response locks = locksResult.result();
                deleteLocks(event, locks);
            } else {
                log.warn("failed to delete all locks. Message: " + locksResult.cause().getMessage());
                event.reply(createErrorReply().put(MESSAGE, locksResult.cause().getMessage()));
            }
        });
    }

    private void deleteLocks(Message<JsonObject> event, Response locks) {
        if (locks == null || locks.size() == 0) {
            event.reply(createOkReply().put(VALUE, 0));
            return;
        }

        List<String> args = new ArrayList<>();
        args.add(locksKey);
        for (Response response : locks) {
            args.add(response.toString());
        }

        redisAPI.hdel(args, delManyResult -> {
            if (delManyResult.succeeded()) {
                log.info("Successfully deleted " + delManyResult.result() + " locks");
                event.reply(createOkReply().put(VALUE, delManyResult.result().toLong()));
            } else {
                log.warn("failed to delete locks. Message: " + delManyResult.cause().getMessage());
                event.reply(createErrorReply().put(MESSAGE, delManyResult.cause().getMessage()));
            }
        });
    }

    private boolean jsonArrayContainsStringsOnly(JsonArray array) {
        for (Object obj : array) {
            if (!(obj instanceof String)) {
                return false;
            }
        }
        return true;
    }

    private void getConfiguration(Message<JsonObject> event) {
        JsonObject result = new JsonObject();
        result.put(RedisquesConfiguration.PROP_ADDRESS, address);
        result.put(RedisquesConfiguration.PROP_CONFIGURATION_UPDATED_ADDRESS, configurationUpdatedAddress);
        result.put(RedisquesConfiguration.PROP_REDIS_PREFIX, redisPrefix);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_ADDRESS, processorAddress);
        result.put(RedisquesConfiguration.PROP_REFRESH_PERIOD, refreshPeriod);
        result.put(RedisquesConfiguration.PROP_REDIS_HOST, redisHost);
        result.put(RedisquesConfiguration.PROP_REDIS_PORT, redisPort);
        result.put(RedisquesConfiguration.PROP_REDIS_AUTH, redisAuth);
        result.put(RedisquesConfiguration.PROP_CHECK_INTERVAL, checkInterval);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_TIMEOUT, processorTimeout);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_DELAY_MAX, processorDelayMax);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_ENABLED, httpRequestHandlerEnabled);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_PREFIX, httpRequestHandlerPrefix);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_PORT, httpRequestHandlerPort);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_USER_HEADER, httpRequestHandlerUserHeader);

        JsonArray queueConfigurationsArray = new JsonArray();
        for (QueueConfiguration queueConfiguration : queueConfigurations) {
            queueConfigurationsArray.add(queueConfiguration.asJsonObject());
        }
        result.put(RedisquesConfiguration.PROP_QUEUE_CONFIGURATIONS, queueConfigurationsArray);

        result.put(RedisquesConfiguration.PROP_REDIS_MAX_POOL_SIZE, redisMaxPoolSize);
        result.put(RedisquesConfiguration.PROP_REDIS_MAX_POOL_WAITING_SIZE, redisMaxPoolWaitingSize);
        result.put(RedisquesConfiguration.PROP_REDIS_MAX_PIPELINE_WAITING_SIZE, redisMaxPipelineWaitingSize);
        result.put(RedisquesConfiguration.PROP_ENABLE_QUEUE_NAME_DECODING, enableQueueNameDecoding);
        result.put(RedisquesConfiguration.PROP_QUEUE_SPEED_INTERVAL_SEC, queueSpeedIntervalSec);
        result.put(RedisquesConfiguration.PROP_MEMORY_USAGE_LIMIT_PCT, memoryUsageLimitPct);

        event.reply(createOkReply().put(VALUE, result));
    }

    private void setConfiguration(Message<JsonObject> event) {
        JsonObject configurationValues = event.body().getJsonObject(PAYLOAD);
        setConfigurationValues(configurationValues, true).onComplete(setConfigurationValuesEvent -> {
            if (setConfigurationValuesEvent.succeeded()) {
                log.debug("About to publish the configuration updates to event bus address '" + configurationUpdatedAddress + "'");
                vertx.eventBus().publish(configurationUpdatedAddress, configurationValues);
                event.reply(setConfigurationValuesEvent.result());
            } else {
                event.reply(createErrorReply().put(MESSAGE, setConfigurationValuesEvent.cause().getMessage()));
            }
        });
    }

    private Future<JsonObject> setConfigurationValues(JsonObject configurationValues, boolean validateOnly) {
        Promise<JsonObject> promise = Promise.promise();

        if (configurationValues != null) {
            List<String> notAllowedConfigurationValues = findNotAllowedConfigurationValues(configurationValues.fieldNames());
            if (notAllowedConfigurationValues.isEmpty()) {
                try {
                    Long processorDelayMaxValue = configurationValues.getLong(PROCESSOR_DELAY_MAX);
                    if (processorDelayMaxValue == null) {
                        promise.fail("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is missing");
                        return promise.future();
                    }
                    if (!validateOnly) {
                        this.processorDelayMax = processorDelayMaxValue;
                        log.info("Updated configuration value of property '" + PROCESSOR_DELAY_MAX + "' to " + processorDelayMaxValue);
                    }
                    promise.complete(createOkReply());
                } catch (ClassCastException ex) {
                    promise.fail("Value for configuration property '" + PROCESSOR_DELAY_MAX + "' is not a number");
                }
            } else {
                String notAllowedConfigurationValuesString = notAllowedConfigurationValues.toString();
                promise.fail("Not supported configuration values received: " + notAllowedConfigurationValuesString);
            }
        } else {
            promise.fail("Configuration values missing");
        }

        return promise.future();
    }

    private List<String> findNotAllowedConfigurationValues(Set<String> configurationValues) {
        if (configurationValues == null) {
            return Collections.emptyList();
        }
        return configurationValues.stream().filter(p -> !ALLOWED_CONFIGURATION_VALUES.contains(p)).collect(Collectors.toList());
    }

    private void registerQueueCheck(RedisquesConfiguration modConfig) {
        vertx.setPeriodic(modConfig.getCheckIntervalTimerMs(), periodicEvent -> luaScriptManager.handleQueueCheck(queueCheckLastexecKey,
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
        consumersMessageConsumer.unregister(event -> uidMessageConsumer.unregister(event1 -> {
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
                    String consumerKey = consumersPrefix + queue;
                    if (log.isTraceEnabled()) {
                        log.trace("RedisQues unregister consumers get: " + consumerKey);
                    }
                    redisAPI.get(consumerKey, event1 -> {
                        if (event1.failed()) {
                            log.warn("Failed to retrieve consumer '{}'.", consumerKey, event1.cause());
                            // IMO we should 'fail()' here. But we don't, to keep backward compatibility.
                        }
                        String consumer = Objects.toString(event1.result(), "");
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
        String keysPattern = consumersPrefix + "*";
        if (log.isTraceEnabled()) {
            log.trace("RedisQues reset consumers keys: " + keysPattern);
        }
        redisAPI.keys(keysPattern, keysResult -> {
            if (keysResult.failed() || keysResult.result() == null) {
                log.error("Unable to get redis keys of consumers", keysResult.cause());
                return;
            }
            Response keys = keysResult.result();
            if (keys == null || keys.size() == 0) {
                log.debug("No consumers found to reset");
                return;
            }
            List<String> args = new ArrayList<>(keys.size());
            for (Response response : keys) {
                args.add(response.toString());
            }
            redisAPI.del(args, delManyResult -> {
                if (delManyResult.succeeded()) {
                    Long count = delManyResult.result().toLong();
                    log.debug("Successfully reset " + count + " consumers");
                } else {
                    log.error("Unable to delete redis keys of consumers");
                }
            });
        });
    }

    private void consume(final String queueName) {
        if (log.isDebugEnabled()) {
            log.debug("RedisQues Requested to consume queue " + queueName);
        }
        refreshRegistration(queueName, event -> {
            if (event.failed()) {
                log.warn("Failed to refresh registration for queue '{}'.", queueName, event.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            // Make sure that I am still the registered consumer
            String consumerKey = consumersPrefix + queueName;
            if (log.isTraceEnabled()) {
                log.trace("RedisQues consume get: " + consumerKey);
            }
            redisAPI.get(consumerKey, event1 -> {
                if (event1.failed()) {
                    log.error("Unable to get consumer for queue " + queueName, event1.cause());
                    return;
                }
                String consumer = Objects.toString(event1.result(), "");
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues refresh registration consumer: " + consumer);
                }
                if (uid.equals(consumer)) {
                    QueueState state = myQueues.get(queueName);
                    if (log.isTraceEnabled()) {
                        log.trace("RedisQues consumer: " + consumer + " queue: " + queueName + " state: " + state);
                    }
                    // Get the next message only once the previous has
                    // been completely processed
                    if (state != QueueState.CONSUMING) {
                        myQueues.put(queueName, QueueState.CONSUMING);
                        if (state == null) {
                            // No previous state was stored. Maybe the
                            // consumer was restarted
                            log.warn("Received request to consume from a queue I did not know about: " + queueName);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("RedisQues Starting to consume queue " + queueName);
                        }
                        readQueue(queueName);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("RedisQues Queue " + queueName + " is already being consumed");
                        }
                    }
                } else {
                    // Somehow registration changed. Let's renotify.
                    log.warn("Registration for queue " + queueName + " has chhas changed to " + consumer);
                    myQueues.remove(queueName);
                    notifyConsumer(queueName);
                }
            });
        });
    }

    private Future<Boolean> isQueueLocked(final String queue) {
        Promise<Boolean> promise = Promise.promise();
        redisAPI.hexists(locksKey, queue, event -> {
            if (event.failed()) {
                log.warn("Failed to check if queue '{}' is locked. Assume no.", queue, event.cause());
                // TODO:  Is it correct, to assume a queue is not locked in case our query failed?
                // Previous implementation assumed this. See "https://github.com/hiddenalpha/vertx-redisques/blob/v2.5.1/src/main/java/org/swisspush/redisques/RedisQues.java#L856".
                promise.complete(Boolean.FALSE);
            } else if (event.result() == null) {
                promise.complete(Boolean.FALSE);
            } else {
                promise.complete(event.result().toInteger() == 1);
            }
        });
        return promise.future();
    }

    private void readQueue(final String queueName) {
        if (log.isTraceEnabled()) {
            log.trace("RedisQues read queue: " + queueName);
        }
        String queueKey = queuesPrefix + queueName;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues read queue lindex: " + queueKey);
        }

        isQueueLocked(queueName).onComplete(lockAnswer -> {
            if (lockAnswer.failed()) {
                log.error("Failed to check if queue '{}' is locked", queueName, lockAnswer.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            boolean locked = lockAnswer.result();
            if (!locked) {
                redisAPI.lindex(queueKey, "0", answer -> {
                    if (answer.failed()) {
                        log.error("Failed to peek queue '{}'", queueName, answer.cause());
                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("RedisQues read queue lindex result: " + answer.result());
                    }
                    if (answer.result() != null) {
                        processMessageWithTimeout(queueName, answer.result().toString(), success -> {

                            // update the queue failure count and get a retry interval
                            int retryInterval = updateQueueFailureCountAndGetRetryInterval(queueName, success);

                            if (success) {
                                // Remove the processed message from the queue
                                if (log.isTraceEnabled()) {
                                    log.trace("RedisQues read queue lpop: " + queueKey);
                                }
                                redisAPI.lpop(Collections.singletonList(queueKey), jsonAnswer -> {
                                    if (jsonAnswer.failed()) {
                                        log.error("Failed to pop from queue '{}'", queueName, jsonAnswer.cause());
                                        // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                                    }
                                    log.debug("RedisQues Message removed, queue " + queueName + " is ready again");
                                    myQueues.put(queueName, QueueState.READY);
                                    // Notify that we are stopped in case it was the last active consumer
                                    if (stoppedHandler != null) {
                                        unregisterConsumers(false);
                                        if (myQueues.isEmpty()) {
                                            stoppedHandler.handle(null);
                                        }
                                    }
                                    // Issue notification to consume next message if any
                                    if (log.isTraceEnabled()) {
                                        log.trace("RedisQues read queue: " + queueKey);
                                    }
                                    redisAPI.llen(queueKey, answer1 -> {
                                        if (answer1.succeeded() && answer1.result() != null && answer1.result().toInteger() > 0) {
                                            notifyConsumer(queueName);
                                        }
                                    });
                                });
                            } else {
                                // Failed. Message will be kept in queue and retried later
                                if (log.isDebugEnabled()) {
                                    log.debug("RedisQues Processing failed for queue " + queueName);
                                    // reschedule
                                    log.debug("RedisQues will re-send the message to queue '" + queueName + "' in " + retryInterval + " seconds");
                                }
                                rescheduleSendMessageAfterFailure(queueName, retryInterval);
                            }
                        });
                    } else {
                        // This can happen when requests to consume happen at the same moment the queue is emptied.
                        if (log.isDebugEnabled()) {
                            log.debug("Got a request to consume from empty queue " + queueName);
                        }
                        myQueues.put(queueName, QueueState.READY);
                    }
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Got a request to consume from locked queue " + queueName);
                }
                myQueues.put(queueName, QueueState.READY);
            }
        });
    }

    private void rescheduleSendMessageAfterFailure(final String queueName, int retryInSeconds) {
        if (log.isTraceEnabled()) {
            log.trace("RedsQues reschedule after failure for queue: " + queueName);
        }

        vertx.setTimer(retryInSeconds * 1000, timerId -> {
            if (log.isDebugEnabled()) {
                log.debug("RedisQues re-notify the consumer of queue '" + queueName + "' at " + new Date(System.currentTimeMillis()));
            }
            notifyConsumer(queueName);

            // reset the queue state to be consumed by {@link RedisQues#consume(String)}
            myQueues.put(queueName, QueueState.READY);
        });
    }

    private void processMessageWithTimeout(final String queue, final String payload, final Handler<Boolean> handler) {
        if (processorDelayMax > 0) {
            log.info("About to process message for queue " + queue + " with a maximum delay of " + processorDelayMax + "ms");
        }
        timer.executeDelayedMax(processorDelayMax).onComplete(delayed -> {
            if (delayed.failed()) {
                log.error("Delayed execution has failed.", delayed.cause());
                // TODO: May we should call handler with failed state now.
                return;
            }
            final EventBus eb = vertx.eventBus();
            JsonObject message = new JsonObject();
            message.put("queue", queue);
            message.put(PAYLOAD, payload);
            if (log.isTraceEnabled()) {
                log.trace("RedisQues process message: " + message + " for queue: " + queue + " send it to processor: " + processorAddress);
            }

            // send the message to the consumer
            DeliveryOptions options = new DeliveryOptions().setSendTimeout(processorTimeout);
            eb.request(processorAddress, message, options, (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                boolean success;
                if (reply.succeeded()) {
                    success = OK.equals(reply.result().body().getString(STATUS));
                } else {
                    log.info("RedisQues QUEUE_ERROR: Consumer failed " + uid + " queue: " + queue + " (" + reply.cause().getMessage() + ")");
                    success = Boolean.FALSE;
                }
                handler.handle(success);
            });
            updateTimestamp(queue, null);
        });
    }

    private void notifyConsumer(final String queueName) {
        log.debug("RedisQues Notifying consumer of queue " + queueName);
        final EventBus eb = vertx.eventBus();

        // Find the consumer to notify
        String key = consumersPrefix + queueName;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues notify consumer get: " + key);
        }
        redisAPI.get(key, event -> {
            if (event.failed()) {
                log.warn("Failed to get consumer for queue '{}'", queueName, event.cause());
                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
            }
            String consumer = Objects.toString(event.result(), null);
            if (log.isTraceEnabled()) {
                log.trace("RedisQues got consumer: " + consumer);
            }
            if (consumer == null) {
                // No consumer for this queue, let's make a peer become consumer
                if (log.isDebugEnabled()) {
                    log.debug("RedisQues Sending registration request for queue " + queueName);
                }
                eb.send(address + "-consumers", queueName);
            } else {
                // Notify the registered consumer
                log.debug("RedisQues Notifying consumer " + consumer + " to consume queue " + queueName);
                eb.send(consumer, queueName);
            }
        });
    }

    private void refreshRegistration(String queueName, Handler<AsyncResult<Response>> handler) {
        if (log.isDebugEnabled()) {
            log.debug("RedisQues Refreshing registration of queue " + queueName + ", expire in " + consumerLockTime + " s");
        }
        String consumerKey = consumersPrefix + queueName;
        if (handler == null) {
            redisAPI.expire(List.of(consumerKey, String.valueOf(consumerLockTime)));
        } else {
            redisAPI.expire(List.of(consumerKey, String.valueOf(consumerLockTime)), handler);
        }
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queueName name of the queue
     * @param handler   (optional) To get informed when done.
     */
    private void updateTimestamp(final String queueName, Handler<AsyncResult<Response>> handler) {
        long ts = System.currentTimeMillis();
        if (log.isTraceEnabled()) {
            log.trace("RedisQues update timestamp for queue: " + queueName + " to: " + ts);
        }
        if (handler == null) {
            redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName));
        } else {
            redisAPI.zadd(Arrays.asList(queuesKey, String.valueOf(ts), queueName), handler);
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
        redisAPI.zrangebyscore(Arrays.asList(queuesKey, "-inf", String.valueOf(limit)), answer -> {
            Response queues = answer.result();
            if (answer.failed() || queues == null) {
                log.error("RedisQues is unable to get list of queues", answer.cause());
                return;
            }
            final AtomicInteger counter = new AtomicInteger(queues.size());
            if (log.isTraceEnabled()) {
                log.trace("RedisQues update queues: " + counter);
            }
            for (Response queueObject : queues) {
                // Check if the inactive queue is not empty (i.e. the key exists)
                final String queueName = queueObject.toString();
                String key = queuesPrefix + queueName;
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues update queue: " + key);
                }
                redisAPI.exists(Collections.singletonList(key), event -> {
                    if (event.failed() || event.result() == null) {
                        log.error("RedisQues is unable to check existence of queue " + queueName, event.cause());
                        return;
                    }
                    if (event.result().toLong() == 1) {
                        log.debug("Updating queue timestamp for queue '{}'", queueName);
                        // If not empty, update the queue timestamp to keep it in the sorted set.
                        updateTimestamp(queueName, result -> {
                            if (result.failed()) {
                                log.warn("Failed to update timestamps for queue '{}'", queueName, result.cause());
                                // We should return here. See: "https://softwareengineering.stackexchange.com/a/190535"
                            }
                            // Ensure we clean the old queues after having updated all timestamps
                            if (counter.decrementAndGet() == 0) {
                                removeOldQueues(limit);
                            }
                        });
                        // Make sure its TTL is correctly set (replaces the previous orphan detection mechanism).
                        refreshRegistration(queueName, null);
                        // And trigger its consumer.
                        notifyConsumer(queueName);
                    } else {
                        // Ensure we clean the old queues also in the case of empty queue.
                        if (log.isTraceEnabled()) {
                            log.trace("RedisQues remove old queue: " + queueName);
                        }
                        if (counter.decrementAndGet() == 0) {
                            removeOldQueues(limit);
                        }
                        queueStatisticsCollector.resetQueueFailureStatistics(queueName);
                    }
                });
            }
        });
    }

    private static JsonObject createOkReply() {
        return new JsonObject().put(STATUS, OK);
    }

    private static JsonObject createErrorReply() {
        return new JsonObject().put(STATUS, ERROR);
    }

    /**
     * Remove queues from the sorted set that are timestamped before a limit time.
     *
     * @param limit limit timestamp
     */
    private void removeOldQueues(long limit) {
        log.debug("Cleaning old queues");
        redisAPI.zremrangebyscore(queuesKey, "-inf", String.valueOf(limit), event -> {
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

    /**
     * find first matching Queue-Configuration
     *
     * @param queueName search first configuration for that queue-name
     * @return null when no queueConfiguration's RegEx matches given queueName - else the QueueConfiguration
     */
    private QueueConfiguration findQueueConfiguration(String queueName) {
        for (QueueConfiguration queueConfiguration : queueConfigurations) {
            if (queueConfiguration.compiledPattern().matcher(queueName).matches()) {
                return queueConfiguration;
            }
        }
        return null;
    }

    /**
     * Retrieve the number of items of the requested queue
     */
    private void getQueueItemsCount(Message<JsonObject> event) {
        String queue = event.body().getJsonObject(PAYLOAD).getString(QUEUENAME);
        redisAPI.llen(queuesPrefix + queue, new GetQueueItemsCountHandler(event));
    }

    /**
     * Retrieve the number of items of many queues
     */
    private void getQueuesItemsCount(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> filterPattern = MessageUtil.extractFilterPattern(event);
        getQueuesItemsCount(event, filterPattern);
    }

    /**
     * Retrieve the size of the queues matching the given filter pattern
     */
    private void getQueuesItemsCount(Message<JsonObject> event, Result<Optional<Pattern>, String> filterPatternResult) {
        if (filterPatternResult.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT)
                    .put(MESSAGE, filterPatternResult.getErr()));
        } else {
            redisAPI.zrangebyscore(List.of(queuesKey, String.valueOf(getMaxAgeTimestamp()), "+inf"),
                    new GetQueuesItemsCountHandler(event, filterPatternResult.getOk(), luaScriptManager,
                            queuesPrefix));
        }
    }

    /**
     * Retrieve the queue statistics info of the requested queues
     *
     * @param event
     */
    private void getQueuesStatistics(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> filterPattern = MessageUtil.extractFilterPattern(event);
        getQueuesStatistics(event, filterPattern);
    }

    /**
     * Retrieve the queue statistics info of the requested queues filtered by the
     * given filter pattern.
     */
    private void getQueuesStatistics(Message<JsonObject> event,
                                     Result<Optional<Pattern>, String> filterPattern) {
        if (filterPattern.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT)
                    .put(MESSAGE, filterPattern.getErr()));
        } else {
            // retrieve all currently known queues from storage and pass this to the handler
            redisAPI.zrangebyscore(List.of(queuesKey, String.valueOf(getMaxAgeTimestamp()), "+inf"),
                    new GetQueuesStatisticsHandler(event, filterPattern.getOk(),
                            queueStatisticsCollector));
        }
    }

    /**
     * Retrieve the summarized queue speed of the requested queues
     *
     * @param event
     */
    private void getQueuesSpeed(Message<JsonObject> event) {
        Result<Optional<Pattern>, String> filterPattern = MessageUtil.extractFilterPattern(event);
        getQueuesSpeed(event, filterPattern);
    }

    /**
     * Retrieve the summarized queue speed of the requested queues filtered by the
     * given filter pattern.
     */
    private void getQueuesSpeed(Message<JsonObject> event,
                                Result<Optional<Pattern>, String> filterPattern) {
        if (filterPattern.isErr()) {
            event.reply(createErrorReply().put(ERROR_TYPE, BAD_INPUT)
                    .put(MESSAGE, filterPattern.getErr()));
        } else {
            // retrieve all currently known queues from storage and pass this to the handler
            redisAPI.zrangebyscore(List.of(queuesKey, String.valueOf(getMaxAgeTimestamp()), "+inf"),
                    new GetQueuesSpeedHandler(event, filterPattern.getOk(),
                            queueStatisticsCollector));
        }
    }

}
