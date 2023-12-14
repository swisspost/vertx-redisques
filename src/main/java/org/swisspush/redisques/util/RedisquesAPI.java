package org.swisspush.redisques.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class RedisquesAPI listing the operations and response values which are supported in Redisques.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesAPI {
    public static final String OK = "ok";
    public static final String INFO = "info";
    public static final String INDEX = "index";
    public static final String LIMIT = "limit";
    public static final String VALUE = "value";
    public static final String ERROR = "error";
    public static final String ERROR_TYPE = "errorType";
    public static final String BAD_INPUT = "bad input";
    public static final String BUFFER = "buffer";
    public static final String STATUS = "status";
    public static final String MESSAGE = "message";

    public static final String MEMORY_FULL = "memory usage limit reached";
    public static final String PAYLOAD = "payload";
    public static final String QUEUENAME = "queuename";
    public static final String FILTER = "filter";
    public static final String COUNT = "count";
    public static final String LOCKS = "locks";
    public static final String QUEUES = "queues";
    public static final String UNLOCK = "unlock";
    public static final String OPERATION = "operation";
    public static final String REQUESTED_BY = "requestedBy";
    public static final String TIMESTAMP = "timestamp";
    public static final String BULK_DELETE = "bulkDelete";
    public static final String NO_SUCH_LOCK = "No such lock";
    public static final String PROCESSOR_DELAY_MAX = "processorDelayMax";
    public static final String PROCESSOR_TIMEOUT = "processorTimeout";

    public static final String MONITOR_QUEUE_NAME = "name";
    public static final String MONITOR_QUEUE_SIZE = "size";
    public static final String STATISTIC_QUEUE_LAST_DEQUEUE_ATTEMPT = "lastDequeueAttempt";
    public static final String STATISTIC_QUEUE_LAST_DEQUEUE_SUCCESS = "lastDequeueSuccess";
    public static final String STATISTIC_QUEUE_NEXT_DEQUEUE_DUE_TS = "nextDequeueDueTimestamp";
    public static final String STATISTIC_QUEUE_FAILURES = "failures";
    public static final String STATISTIC_QUEUE_BACKPRESSURE = "backpressureTime";
    public static final String STATISTIC_QUEUE_SLOWDOWN = "slowdownTime";
    public static final String STATISTIC_QUEUE_SPEED = "speed";
    public final static String STATISTIC_QUEUE_DEQUEUESTATISTIC = "dequeueStatistic";
    public static final String STATISTIC_QUEUE_SPEED_INTERVAL_UNIT= "unitSec";

    private static final Logger log = LoggerFactory.getLogger(RedisquesAPI.class);

    public enum QueueOperation {
        enqueue(null),
        lockedEnqueue(null),
        getConfiguration(null),
        setConfiguration(null),
        check(null),
        reset(null),
        stop(null),
        getQueueItems("getListRange"),
        addQueueItem("addItem"),
        deleteQueueItem("deleteItem"),
        getQueueItem("getItem"),
        replaceQueueItem("replaceItem"),
        deleteAllQueueItems(null),
        bulkDeleteQueues(null),
        getAllLocks(null),
        putLock(null),
        bulkPutLocks(null),
        getLock(null),
        deleteLock(null),
        deleteAllLocks(null),
        bulkDeleteLocks(null),
        getQueues(null),
        getQueuesCount(null),
        getQueueItemsCount(null),
        getQueuesItemsCount(null),
        getQueuesStatistics(null),
        getQueuesSpeed(null);

        private final String legacyName;

        QueueOperation(String legacyName){
            this.legacyName = legacyName;
        }

        public String getLegacyName() {
            return legacyName;
        }

        public boolean hasLegacyName(){
            return legacyName != null;
        }

        public static QueueOperation fromString(String op){
            for (QueueOperation queueOperation : values()) {
                if(queueOperation.name().equalsIgnoreCase(op)){
                    return queueOperation;
                } else if(queueOperation.hasLegacyName() && queueOperation.getLegacyName().equalsIgnoreCase(op)){
                    log.warn("Legacy queue operation used. This may be removed in future releases. Use '"+queueOperation.name()+"' instead of '" + queueOperation.getLegacyName() + "'");
                    return queueOperation;
                }
            }
            return null;
        }
    }

    public static JsonObject buildOperation(QueueOperation queueOperation){
        JsonObject op = new JsonObject();
        op.put(OPERATION, queueOperation.name());
        return op;
    }

    public static JsonObject buildOperation(QueueOperation queueOperation, JsonObject payload){
        JsonObject op = buildOperation(queueOperation);
        op.put(PAYLOAD, payload);
        return op;
    }

    public static JsonObject buildGetConfigurationOperation() { return buildOperation(QueueOperation.getConfiguration); }

    public static JsonObject buildSetConfigurationOperation(JsonObject configuration) { return buildOperation(QueueOperation.setConfiguration, configuration); }

    public static JsonObject buildCheckOperation(){ return buildOperation(QueueOperation.check); }

    public static JsonObject buildEnqueueOperation(String queueName, String message){
        JsonObject operation = buildOperation(QueueOperation.enqueue, new JsonObject().put(QUEUENAME, queueName));
        operation.put(MESSAGE, message);
        return operation;
    }

    public static JsonObject buildLockedEnqueueOperation(String queueName, String message, String lockRequestedBy){
        JsonObject operation = buildOperation(QueueOperation.lockedEnqueue, new JsonObject().put(QUEUENAME, queueName).put(REQUESTED_BY, lockRequestedBy));
        operation.put(MESSAGE, message);
        return operation;
    }

    public static JsonObject buildGetQueueItemsOperation(String queueName, String limit){
        return buildOperation(QueueOperation.getQueueItems, new JsonObject().put(QUEUENAME, queueName).put(LIMIT, limit));
    }

    public static JsonObject buildAddQueueItemOperation(String queueName, String buffer){
        return buildOperation(QueueOperation.addQueueItem, new JsonObject().put(QUEUENAME, queueName).put(BUFFER, buffer));
    }

    public static JsonObject buildGetQueueItemOperation(String queueName, int index){
        return buildOperation(QueueOperation.getQueueItem, new JsonObject().put(QUEUENAME, queueName).put(INDEX, index));
    }

    public static JsonObject buildReplaceQueueItemOperation(String queueName, int index, String buffer){
        return buildOperation(QueueOperation.replaceQueueItem, new JsonObject().put(QUEUENAME, queueName).put(INDEX, index).put(BUFFER, buffer));
    }

    public static JsonObject buildDeleteQueueItemOperation(String queueName, int index){
        return buildOperation(QueueOperation.deleteQueueItem, new JsonObject().put(QUEUENAME, queueName).put(INDEX, index));
    }

    public static JsonObject buildDeleteAllQueueItemsOperation(String queueName){
        return buildDeleteAllQueueItemsOperation(queueName, false);
    }

    public static JsonObject buildDeleteAllQueueItemsOperation(String queueName, boolean unlock){
        return buildOperation(QueueOperation.deleteAllQueueItems, new JsonObject().put(QUEUENAME, queueName).put(UNLOCK, unlock));
    }

    public static JsonObject buildBulkDeleteQueuesOperation(JsonArray queuesToDelete){
        return buildOperation(QueueOperation.bulkDeleteQueues, new JsonObject().put(QUEUES, queuesToDelete));
    }

    public static JsonObject buildGetQueuesOperation(){
        return buildOperation(QueueOperation.getQueues);
    }

    /**
     * @param filterPattern
     *      Filter pattern. Method handles {@code null} gracefully.
     */
    public static JsonObject buildGetQueuesOperation(String filterPattern) {
        if (filterPattern != null) {
            return buildOperation(QueueOperation.getQueues, new JsonObject().put(FILTER, filterPattern));
        } else {
            return buildOperation(QueueOperation.getQueues);
        }
    }

    /**
     * Evaluate the size of all queues matching the optional given filter pattern.
     */
    public static JsonObject buildGetQueuesCountOperation(){
        return buildOperation(QueueOperation.getQueuesCount);
    }

    /**
     * Evaluate the size of all queues matching the optional given filter pattern.
     * @param filterPattern
     *      Filter pattern. Method handles {@code null} gracefully.
     */
    public static JsonObject buildGetQueuesCountOperation(String filterPattern) {
        if (filterPattern != null) {
            return buildOperation(QueueOperation.getQueuesCount, new JsonObject().put(FILTER, filterPattern));
        } else {
            return buildOperation(QueueOperation.getQueuesCount);
        }
    }


    /**
     * Evaluate the size of the given queue
     * @param queueName the name of the queue to be evaluated
     * @return the evaluated size
     */
    public static JsonObject buildGetQueueItemsCountOperation(String queueName){
        return buildOperation(QueueOperation.getQueueItemsCount, new JsonObject().put(QUEUENAME, queueName));
    }

    /**
     * Evaluate the size of the ques according to the given filter
     */
    public static JsonObject buildGetQueuesItemsCountOperation(String filter){
        return buildOperation(QueueOperation.getQueuesItemsCount, new JsonObject().put(FILTER, filter));
    }

     public static JsonObject buildGetLockOperation(String queueName){
        return buildOperation(QueueOperation.getLock, new JsonObject().put(QUEUENAME, queueName));
    }

    public static JsonObject buildDeleteLockOperation(String queueName){
        return buildOperation(QueueOperation.deleteLock, new JsonObject().put(QUEUENAME, queueName));
    }

    public static JsonObject buildDeleteAllLocksOperation(){
        return buildOperation(QueueOperation.deleteAllLocks);
    }

    public static JsonObject buildBulkDeleteLocksOperation(JsonArray locksToDelete){
        return buildOperation(QueueOperation.bulkDeleteLocks, new JsonObject().put(LOCKS, locksToDelete));
    }

    public static JsonObject buildPutLockOperation(String queueName, String user){
        return buildOperation(QueueOperation.putLock, new JsonObject().put(QUEUENAME, queueName).put(REQUESTED_BY, user));
    }

    public static JsonObject buildBulkPutLocksOperation(JsonArray locksToPut, String user){
        return buildOperation(QueueOperation.bulkPutLocks, new JsonObject().put(LOCKS, locksToPut).put(REQUESTED_BY, user));
    }

    public static JsonObject buildGetAllLocksOperation(){
        return buildOperation(QueueOperation.getAllLocks);
    }

    /**
     * @param filterPattern
     *      Filter pattern. Method handles {@code null} gracefully.
     */
    public static JsonObject buildGetAllLocksOperation(String filterPattern) {
        if (filterPattern != null) {
            return buildOperation(QueueOperation.getAllLocks, new JsonObject().put(FILTER, filterPattern));
        } else {
            return buildGetAllLocksOperation();
        }
    }

    public static JsonObject buildGetQueuesStatisticsOperation() {
        return buildOperation(QueueOperation.getQueuesStatistics);
    }

    /**
     * @param filterPattern
     *      Filter pattern. Method handles {@code null} gracefully.
     */
    public static JsonObject buildGetQueuesStatisticsOperation(String filterPattern) {
        if (filterPattern != null) {
            return buildOperation(QueueOperation.getQueuesStatistics, new JsonObject().put(FILTER, filterPattern));
        }
        return buildGetQueuesStatisticsOperation();
    }

    /**
     *
     * Retrieve total speed over all queues if no filter given
     * @return The total speed overall
     */
    public static JsonObject buildGetQueuesSpeedOperation() {
        return buildOperation(QueueOperation.getQueuesSpeed);
    }

    /**
     * Retrieve speed for all the queues matching the given filter
     * @param filterPattern The queues filter for which we would like to get the total speed
     *      Filter pattern. Method handles {@code null} gracefully.
     */
    public static JsonObject buildGetQueuesSpeedOperation(String filterPattern) {
        if (filterPattern != null) {
            return buildOperation(QueueOperation.getQueuesSpeed, new JsonObject().put(FILTER, filterPattern));
        }
        return buildGetQueuesSpeedOperation();
    }


}
