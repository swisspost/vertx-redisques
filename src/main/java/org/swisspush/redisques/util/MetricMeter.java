package org.swisspush.redisques.util;

public enum MetricMeter {

    ENQUEUE_SUCCESS("redisques.enqueue.success", "Overall count of queue items to be enqueued successfully"),
    ENQUEUE_FAIL("redisques.enqueue.fail", "Overall count of queue items which could not be enqueued"),
    DEQUEUE("redisques.dequeue", "Overall count of queue items to be dequeued from the queues"),
    ACTIVE_QUEUES("redisques.active.queues", "Count of active queues"),
    MAX_QUEUE_SIZE("redisques.max.queue.size", "Amount of queue items of the biggest queue"),
    QUEUE_STATE_READY_SIZE("redisques.queue.state.ready.size", "Amount of queue in state ready"),
    QUEUE_STATE_CONSUMING_SIZE("redisques.queue.state.consuming.size", "Amount of queue in state consuming"),
    ALIVE_CONSUMER("redisques.alive.consumers", "Count of consumer registered and alive for a redisques instance"),
    QUEUE_CONSUMING("redisques.queue.consuming", "Count of consumer registered and alive for a redisques instance");

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
