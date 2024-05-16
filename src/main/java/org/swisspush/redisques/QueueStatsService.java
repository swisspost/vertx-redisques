package org.swisspush.redisques;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.NoStacktraceException;
import org.swisspush.redisques.util.DequeueStatistic;
import org.swisspush.redisques.util.DequeueStatisticCollector;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static java.lang.Long.compare;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyList;
import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_NAME;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_SIZE;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUES;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.buildGetQueuesItemsCountOperation;


/**
 * <p>Old impl did fetch all queues (take 2000 as an example from PaISA prod) Did
 * a nested iteration, so worst case iterate 2000 times 2000 queues to find the
 * matching queue (2'000 * 2'000 = 4'000'000 iterations). New impl now does setup a
 * dictionary then does indexed access while iterating the 2000 entries
 * (2'000 + 2'000 = 4'000 iterations). Keep in mind 2'000 is just some usual value.
 * Under load, this value can increase further and so the old, exponential approach
 * just did explode in terms of computational efford.</p>
 */
public class QueueStatsService {

    private static final Logger log = getLogger(QueueStatsService.class);
    private final Vertx vertx;
    private final EventBus eventBus;
    private final String redisquesAddress;
    private final QueueStatisticsCollector queueStatisticsCollector;
    private final DequeueStatisticCollector dequeueStatisticCollector;
    private final Semaphore incomingRequestQuota;

    public QueueStatsService(
            Vertx vertx,
            EventBus eventBus,
            String redisquesAddress,
            QueueStatisticsCollector queueStatisticsCollector,
            DequeueStatisticCollector dequeueStatisticCollector,
            Semaphore incomingRequestQuota
    ) {
        this.vertx = vertx;
        this.eventBus = eventBus;
        this.redisquesAddress = redisquesAddress;
        this.queueStatisticsCollector = queueStatisticsCollector;
        this.dequeueStatisticCollector = dequeueStatisticCollector;
        this.incomingRequestQuota = incomingRequestQuota;
    }

    public <CTX> void getQueueStats(CTX mCtx, GetQueueStatsMentor<CTX> mentor) {
        if (!incomingRequestQuota.tryAcquire()) {
            var ex = new RuntimeException("Server too busy to handle yet-another-queue-stats-request now");
            vertx.runOnContext(v -> mentor.onError(ex, mCtx));
            return;
        }
        AtomicBoolean isCompleted = new AtomicBoolean();
        try {
            var req0 = new GetQueueStatsRequest<CTX>();
            BiConsumer<Throwable, List<Queue>> onDone = (Throwable ex, List<Queue> ans) -> {
                if (!isCompleted.compareAndSet(false, true)) {
                    if (log.isInfoEnabled()) log.info("", new RuntimeException("onDone MUST be called ONCE only", ex));
                    return;
                }
                incomingRequestQuota.release();
                if (ex != null) mentor.onError(ex, mCtx);
                else mentor.onQueueStatistics(ans, mCtx);
            };
            req0.mCtx = mCtx;
            req0.mentor = mentor;
            fetchQueueNamesAndSize(req0, (ex1, req1) -> {
                if (ex1 != null) { onDone.accept(ex1, null); return; }
                // Prepare a list of queue names as it is needed to fetch retryDetails.
                req1.queueNames = new ArrayList<>(req1.queues.size());
                for (Queue q : req1.queues) req1.queueNames.add(q.name);
                fetchRetryDetails(req1, (ex2, req2) -> {
                    if (ex2 != null) { onDone.accept(ex2, null); return; }
                    attachDequeueStats(req2, (ex3, req3) -> {
                        if (ex3 != null) { onDone.accept(ex3, null); return; }
                        onDone.accept(null, req3.queues);
                    });
                });
            });
        } catch (Exception ex) {
            if (!isCompleted.compareAndSet(false, true)) {
                if (log.isInfoEnabled()) log.info("onDone MUST be called ONCE only", ex);
                return;
            }
            incomingRequestQuota.release();
            vertx.runOnContext(v -> mentor.onError(ex, mCtx));
        }
    }

    private <CTX> void fetchQueueNamesAndSize(GetQueueStatsRequest<CTX> req, BiConsumer<Throwable, GetQueueStatsRequest<CTX>> onDone) {
        String filter = req.mentor.filter(req.mCtx);
        JsonObject operation = buildGetQueuesItemsCountOperation(filter);
        eventBus.<JsonObject>request(redisquesAddress, operation, ev -> {
            if (ev.failed()) {
                onDone.accept(new NoStacktraceException("error_QzkCACMbAgCgOwIA", ev.cause()), req);
                return;
            }
            JsonObject body = ev.result().body();
            String status = body.getString(STATUS);
            if (!OK.equals(status)) {
                onDone.accept(new NoStacktraceException("error_aXACAPcbAgBLGgIABnAC: " + status), null);
                return;
            }
            JsonArray queuesJsonArr = body.getJsonArray(QUEUES);
            if (queuesJsonArr == null || queuesJsonArr.isEmpty()) {
                log.debug("result was {}, we return an empty result.", queuesJsonArr == null ? "null" : "empty");
                req.queues = emptyList();
                onDone.accept(null, req);
                return;
            }
            boolean includeEmptyQueues = req.mentor.includeEmptyQueues(req.mCtx);
            List<Queue> queues = new ArrayList<>(queuesJsonArr.size());
            for (var it = queuesJsonArr.iterator(); it.hasNext(); ) {
                JsonObject queueJson = (JsonObject) it.next();
                String name = queueJson.getString(MONITOR_QUEUE_NAME);
                Long size = queueJson.getLong(MONITOR_QUEUE_SIZE);
                // No need to process empty queues any further if caller is not interested
                // in them anyway.
                if (!includeEmptyQueues && (size == null || size == 0)) continue;
                Queue queue = new Queue(name, size);
                queues.add(queue);
            }
            queues.sort(this::compareLargestFirst);
            // Only the part with the most filled queues got requested. Get rid of
            // all shorter queues then.
            int limit = req.mentor.limit(req.mCtx);
            if (limit != 0 && queues.size() > limit) queues = queues.subList(0, limit);
            req.queues = queues;
            onDone.accept(null, req);
        });
    }

    private <CTX> void fetchRetryDetails(GetQueueStatsRequest<CTX> req, BiConsumer<Throwable, GetQueueStatsRequest<CTX>> onDone) {
        long begGetQueueStatsMs = currentTimeMillis();
        assert req.queueNames != null;
        queueStatisticsCollector.getQueueStatistics(req.queueNames).onComplete( ev -> {
            req.queueNames = null; // <- no longer needed
            long durGetQueueStatsMs = currentTimeMillis() - begGetQueueStatsMs;
            log.debug("queueStatisticsCollector.getQueueStatistics() took {}ms", durGetQueueStatsMs);
            if (ev.failed()) {
                log.warn("queueStatisticsCollector.getQueueStatistics() failed. Fallback to empty result.", ev.cause());
                req.queuesJsonArr = new JsonArray();
                onDone.accept(null, req);
                return;
            }
            JsonObject queStatsJsonObj = ev.result();
            String status = queStatsJsonObj.getString(STATUS);
            if (!OK.equals(status)) {
                log.warn("queueStatisticsCollector.getQueueStatistics() responded '" + status + "'. Fallback to empty result.", ev.cause());
                req.queuesJsonArr = new JsonArray();
                onDone.accept(null, req);
                return;
            }
            req.queuesJsonArr = queStatsJsonObj.getJsonArray(QUEUES);
            onDone.accept(null, req);
        });
    }

    private <CTX> void attachDequeueStats(GetQueueStatsRequest<CTX> req, BiConsumer<Throwable, GetQueueStatsRequest<CTX>> onDone) {
        // Setup a lookup table as we need to find by name further below.
        Map<String, JsonObject> detailsByName = new HashMap<>(req.queuesJsonArr.size());
        for (var it = (Iterator<JsonObject>) (Object) req.queuesJsonArr.iterator(); it.hasNext(); ) {
            JsonObject detailJson = it.next();
            String name = detailJson.getString(MONITOR_QUEUE_NAME);
            detailsByName.put(name, detailJson);
        }

        dequeueStatisticCollector.getAllDequeueStatistics().onSuccess(event -> {
            for (Queue queue : req.queues) {
                if (event.containsKey(queue.name)) {
                    DequeueStatistic sharedDequeueStatisticCopy = event.get(queue.name);
                    // Attach value of shared data
                    queue.lastDequeueAttemptEpochMs = sharedDequeueStatisticCopy.getLastDequeueAttemptTimestamp();
                    queue.lastDequeueSuccessEpochMs = sharedDequeueStatisticCopy.getLastDequeueSuccessTimestamp();
                    queue.nextDequeueDueTimestampEpochMs = sharedDequeueStatisticCopy.getNextDequeueDueTimestamp();
                }
            }
            onDone.accept(null, req);
        }).onFailure(throwable -> {
            log.warn("queueStatisticsCollector.getAllDequeueStatistics() failed. Fallback to empty result.", throwable);
            onDone.accept(null, req);
        });
    }

    private int compareLargestFirst(Queue a, Queue b) {
        return compare(b.size, a.size);
    }


    private static class GetQueueStatsRequest<CTX> {
        private CTX mCtx;
        private GetQueueStatsMentor<CTX> mentor;
        private List<String> queueNames;
        private JsonArray queuesJsonArr;
        private List<Queue> queues;
    }


    public static class Queue {
        private final String name;
        private final long size;
        private Long lastDequeueAttemptEpochMs;
        private Long lastDequeueSuccessEpochMs;
        private Long nextDequeueDueTimestampEpochMs;
        private Queue(String name, long size){
            assert name != null;
            this.name = name;
            this.size = size;
        }

        public String getName() { return name; }
        public long getSize() { return size; }
        public Long getLastDequeueAttemptEpochMs() { return lastDequeueAttemptEpochMs; }
        public Long getLastDequeueSuccessEpochMs() { return lastDequeueSuccessEpochMs; }
        public Long getNextDequeueDueTimestampEpochMs() { return nextDequeueDueTimestampEpochMs; }
    }


    /**
     * <p>Mentors fetching operations and so provides the fetcher the required
     * information. Finally it also receives the operations result.</p>
     *
     * @param <CTX>
     *     The context object of choice handled back to each callback so the mentor
     *     knows about what request the fetcher is talking.
     */
    public static interface GetQueueStatsMentor<CTX> {

        /**
         * <p>Returning true means that all queues will be present in the result. If
         * false, empty queues won't show up the result.</p>
         *
         * @param ctx  See {@link GetQueueStatsMentor}.
         */
        public boolean includeEmptyQueues( CTX ctx );

        /** <p>Limits the result to the largest N queues.</p> */
        public int limit( CTX ctx );

        public String filter( CTX ctx);

        /** <p>Called ONCE with the final result.</p> */
        public void onQueueStatistics(List<Queue> queues, CTX ctx);

        /**
         * <p>Called as soon an error occurs. After an error occurred, {@link #onQueueStatistics(List, Object)}
         * will NOT be called, as the operation did fail.</p>
         */
        public void onError(Throwable ex, CTX ctx);
    }

}
