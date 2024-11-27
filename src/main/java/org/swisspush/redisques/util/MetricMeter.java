package org.swisspush.redisques.util;

public enum MetricMeter {

    ENQUEUE_SUCCESS("redisques.enqueue.success", "Overall count of queue items to be enqueued successfully"),
    ENQUEUE_FAIL("redisques.enqueue.fail", "Overall count of queue items which could not be enqueued"),
    DEQUEUE("redisques.dequeue", "Overall count of queue items to be dequeued from the queues"),
    ACTIVE_QUEUES("redisques.active.queues", "Count of active queues"),
    MAX_QUEUE_SIZE("redisques.max.queue.size", "Amount of queue items of the biggest queue");

    private final String id;
    private final String description;

    MetricMeter(String id, String description) {
        this.id = id;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }
}
