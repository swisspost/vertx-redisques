package org.swisspush.redisques;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
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
import org.swisspush.redisques.handler.*;
import org.swisspush.redisques.util.RedisquesConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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

    // Address of the redis mod
    private RedisClient redisClient;

    // Prefix for redis keys holding queues and consumers.
    private String redisPrefix = "redisques:";

    // Prefix for queues
    private String queuesPrefix = redisPrefix + "queues:";

    // Prefix for consumers
    private String consumersPrefix = "consumers:";

    private String redisques_locks = "redisques:locks";

    // Address of message processors
    private String processorAddress = "redisques-processor";

    public static final String STATUS = "status";
    public static final String MESSAGE = "message";
    public static final String VALUE = "value";
    public static final String INFO = "info";
    public static final String OK = "ok";
    public static final String ERROR = "error";
    public static final String PAYLOAD = "payload";
    public static final String QUEUE_NAME = "queuename";
    public static final String REQUESTED_BY = "requestedBy";
    public static final String TIMESTAMP = "timestamp";
    public static final String LIMIT = "limit";
    public static final String INDEX = "index";
    public static final String BUFFER = "buffer";
    public static final String OPERATION = "operation";

    // Consumers periodically refresh their subscription while they are
    // consuming.
    private int refreshPeriod = 10;

    // the time we wait for the processor to answer, before we cancel processing
    private int processorTimeout = 240000;

    private static final int DEFAULT_MAX_QUEUEITEM_COUNT = 49;

    // Handler receiving registration requests when no consumer is registered
    // for a queue.
    private Handler<Message<String>> registrationRequestHandler = event -> {
        final EventBus eb = vertx.eventBus();
        final String queue = event.body();
        log.debug("RedisQues Got registration request for queue " + queue + " from consumer: " + uid);
        // Try to register for this queue
        redisClient.setnx(redisPrefix + consumersPrefix + queue, uid, event1 -> {
            long value = event1.result();
            if (log.isTraceEnabled()) {
                log.trace("RedisQues setxn result: " + value + " for queue: " + queue);
            }
            if (value == 1L) {
                // I am now the registered consumer for this queue.
                log.debug("RedisQues Now registered for queue " + queue);
                myQueues.put(queue, QueueState.READY);
                consume(queue);
            } else {
                log.debug("RedisQues Missed registration for queue " + queue);
                // Someone else just became the registered consumer. I
                // give up.
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
        redisPrefix = modConfig.getRedisPrefix();
        processorAddress = modConfig.getProcessorAddress();
        refreshPeriod = modConfig.getRefreshPeriod();

        this.redisClient = RedisClient.create(vertx, new RedisOptions()
                .setHost(modConfig.getRedisHost())
                .setPort(modConfig.getRedisPort())
                .setEncoding(modConfig.getRedisEncoding()));

        // Handles operations
        eb.localConsumer(address, new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> event) {
                String operation = event.body().getString(OPERATION);
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues got operation:" + operation);
                }
                switch (operation) {
                    case "enqueue":
                        updateTimestamp(event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME), null);
                        String keyEnqueue = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                        String valueEnqueue = event.body().getString(MESSAGE);
                        redisClient.rpush(keyEnqueue, valueEnqueue, event2 -> {
                            JsonObject reply = new JsonObject();
                            if(event2.succeeded()){
                                log.debug("RedisQues Enqueued message into queue " + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME));
                                notifyConsumer(event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME));
                                reply.put(STATUS, OK);
                                reply.put(MESSAGE, "enqueued");
                                event.reply(reply);
                            } else {
                                String message = "RedisQues QUEUE_ERROR: Error while enqueing message into queue " + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                                log.error(message);
                                reply.put(STATUS, ERROR);
                                reply.put(MESSAGE, message);
                                event.reply(reply);
                            }
                        });
                        break;
                    case "check":
                        checkQueues();
                        break;
                    case "reset":
                        resetConsumers();
                        break;
                    case "stop":
                        gracefulStop(event1 -> {
                            JsonObject reply = new JsonObject();
                            reply.put(STATUS, OK);
                        });
                        break;
                    case "getListRange":
                        String keyListRange = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                        int maxQueueItemCountIndex = getMaxQueueItemCountIndex(event.body().getJsonObject(PAYLOAD).getString(LIMIT));
                        redisClient.llen(keyListRange, countReply -> {
                            redisClient.lrange(keyListRange, 0, maxQueueItemCountIndex, new GetListRangeHandler(event, countReply.result()));
                        });
                        break;
                    case "addItem":
                        String key1 = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                        String valueAddItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
                        redisClient.rpush(key1, valueAddItem, new AddItemHandler(event));
                        break;
                    case "deleteItem":
                        String keyLset = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                        int indexLset = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
                        redisClient.lset(keyLset, indexLset, "TO_DELETE", event1 -> {
                            if(event1.succeeded()){
                                String keyLrem = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                                redisClient.lrem(keyLrem, 0, "TO_DELETE", replyLrem -> event.reply(new JsonObject().put(STATUS, OK)));
                            } else {
                                event.reply(new JsonObject().put(STATUS, ERROR));
                            }
                        });
                        break;
                    case "getItem":
                        String key = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                        int index = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
                        redisClient.lindex(key, index, new GetItemHandler(event));
                        break;
                    case "replaceItem":
                        String keyReplaceItem = queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME);
                        int indexReplaceItem = event.body().getJsonObject(PAYLOAD).getInteger(INDEX);
                        String bufferReplaceItem = event.body().getJsonObject(PAYLOAD).getString(BUFFER);
                        redisClient.lset(keyReplaceItem, indexReplaceItem, bufferReplaceItem, new ReplaceItemHandler(event));
                        break;
                    case "deleteAllQueueItems":
                        redisClient.del(queuesPrefix + event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME), new DeleteAllQueueItems(event));
                        break;
                    case "getAllLocks":
                        redisClient.hkeys(redisques_locks, new GetAllLocksHandler(event));
                        break;
                    case "putLock":
                        JsonObject lockInfo = extractLockInfo(event.body().getJsonObject(PAYLOAD).getString(REQUESTED_BY));
                        if (lockInfo != null) {
                            redisClient.hmset(redisques_locks, new JsonObject().put(event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME), lockInfo.encode()),
                                    new PutLockHandler(event));
                        } else {
                            event.reply(new JsonObject().put(STATUS, ERROR).put(MESSAGE, "Property '" + REQUESTED_BY + "' missing"));
                        }
                        break;
                    case "getLock":
                        redisClient.hget(redisques_locks, event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME),new GetLockHandler(event));
                        break;
                    case "deleteLock":
                        redisClient.hdel(redisques_locks, event.body().getJsonObject(PAYLOAD).getString(QUEUE_NAME), new DeleteLockHandler(event));
                        break;
                }
            }
        });

        // Handles registration requests
        conumersMessageConsumer = eb.consumer(address + "-consumers", registrationRequestHandler);

        // Handles notifications
        uidMessageConsumer = eb.consumer(uid, new Handler<Message<String>>() {
            public void handle(Message<String> event) {
                final String queue = event.body();
                log.debug("RedisQues Got notification for queue " + queue);
                consume(queue);
            }
        });

        // Periodic refresh of my registrations on active queues.
        vertx.setPeriodic(refreshPeriod * 1000, event -> {
            // Check if I am still the registered consumer
            myQueues.entrySet().stream().filter(entry -> entry.getValue() == QueueState.CONSUMING).forEach(entry -> {
                final String queue = entry.getKey();
                // Check if I am still the registered consumer
                String consumerKey = redisPrefix + consumersPrefix + queue;
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
                    String consumerKey = redisPrefix + consumersPrefix + queue;
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
        String keysPattern = redisPrefix + consumersPrefix + "*";
        if (log.isTraceEnabled()) {
            log.trace("RedisQues reset consumers keys: " + keysPattern);
        }
        redisClient.keys(keysPattern, event -> {});
    }

    private void consume(final String queue) {
        log.debug(" RedisQues Requested to consume queue " + queue);
        refreshRegistration(queue, event -> {
            // Make sure that I am still the registered consumer
            String key = redisPrefix + consumersPrefix + queue;
            if (log.isTraceEnabled()) {
                log.trace("RedisQues consume get: " + key);
            }
            redisClient.get(key, event1 -> {
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

    private void readQueue(final String queue) {
        if (log.isTraceEnabled()) {
            log.trace("RedisQues read queue: " + queue);
        }
        String key = queuesPrefix + queue;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues read queue lindex: " + key);
        }

        redisClient.lindex(key, 0, answer -> {
            if (log.isTraceEnabled()) {
                log.trace("RedisQues read queue lindex result: " + answer.result());
            }
            if (answer.result() != null) {
                processMessageWithTimeout(queue, answer.result(), sendResult -> {
                    if (sendResult.success) {
                        // Remove the processed message from the
                        // queue
                        String key1 = queuesPrefix + queue;
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
                            String key2 = queuesPrefix + queue;
                            if (log.isTraceEnabled()) {
                                log.trace("RedisQues read queue: " + key);
                            }
                            redisClient.llen(key2, answer1 -> {
                                if (answer1.result() > 0) {
                                    notifyConsumer(queue);
                                }
                            });
                        });
                    } else {
                        // Failed. Message will be kept in queue and
                        // retried at next wakeup.
                        log.debug("RedisQues Processing failed for queue " + queue);
                        myQueues.put(queue, QueueState.READY);
                        vertx.cancelTimer(sendResult.timeoutId);
                    }
                });
            } else {
                // This can happen when requests to consume happen at the same moment the queue is emptied.
                log.debug("Got a request to consume from empty queue " + queue);
                myQueues.put(queue, QueueState.READY);
            }
        });
    }

    private void processMessageWithTimeout(final String queue, final String payload, final Handler<SendResult> handler) {
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
        eb.send(processorAddress, message, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                Boolean success;
                if(reply.succeeded()){
                    success = OK.equals(reply.result().body().getString(STATUS));
                } else {
                    success = Boolean.FALSE;
                }
                handler.handle(new SendResult(success, timeoutId));
            }
        });
        updateTimestamp(queue, null);
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
        String key = redisPrefix + consumersPrefix + queue;
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
        log.debug("RedisQues Refreshing registration of queue " + queue + ", expire at " + (2 * refreshPeriod));
        String key = redisPrefix + consumersPrefix + queue;
        if (log.isTraceEnabled()) {
            log.trace("RedisQues refresh registration: " + key);
        }
        if (handler != null) {
            redisClient.expire(key, 2 * refreshPeriod, handler);
        } else {
            redisClient.expire(key, 2 * refreshPeriod, event -> {});
        }
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queue
     * @param handler (optional) To get informed when done.
     */
    private void updateTimestamp(final String queue, Handler<AsyncResult<Long>> handler) {
        if (log.isTraceEnabled()) {
            log.trace("RedisQues update timestamp for queue: " + queue + " to: " + System.currentTimeMillis());
        }
        if (handler != null) {
            redisClient.zadd(redisPrefix + "queues", System.currentTimeMillis(), queue, handler);
        } else {
            redisClient.zadd(redisPrefix + "queues", System.currentTimeMillis(), queue, event -> {});
        }
    }

    /**
     * Notify not-active/not-empty queues to be processed (e.g. after a reboot).
     * Cleanup timestamps of not-active/empty queues.
     * This uses a sorted set of queue names scored by last update timestamp.
     */
    private void checkQueues() {
        log.debug("Checking queues timestamps");
        // List all queues that look inactive (i.e. that have not been updated since 3 periods).
        final long limit = System.currentTimeMillis() - 3 * refreshPeriod * 1000;
        redisClient.zrangebyscore(redisPrefix + "queues","-inf", String.valueOf(limit), RangeLimitOptions.NONE, answer -> {
            JsonArray queues = answer.result();
            final AtomicInteger counter = new AtomicInteger(queues.size());
            if (log.isTraceEnabled()) {
                log.trace("RedisQues update queues: " + counter);
            }
            for (Object queueObject : queues) {
                // Check if the inactive queue is not empty (i.e. the key exists)
                final String queue = (String) queueObject;
                String key = queuesPrefix + queue;
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues update queue: " + key);
                }
                redisClient.exists(key, event -> {
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
     * @param limit
     */
    private void removeOldQueues(long limit) {
        log.debug("Cleaning old queues");
        redisClient.zremrangebyscore(redisPrefix + "queues", "-inf", String.valueOf(limit), event -> {});
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