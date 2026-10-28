package org.swisspush.redisques.util;

public enum MetricMeter {

    ENQUEUE_SUCCESS("redisques.enqueue.success", "Overall count of queue items to be enqueued successfully"),
    ENQUEUE_FAIL("redisques.enqueue.fail", "Overall count of queue items which could not be enqueued"),
    DEQUEUE("redisques.dequeue", "Overall count of queue items to be dequeued from the queues"),
    ACTIVE_QUEUES("redisques.active.queues", "Count of active queues"),
    MAX_QUEUE_SIZE("redisques.max.queue.size", "Amount of queue items of the biggest queue"),
    QUEUE_STATE_READY_SIZE("redisques.queue.state.ready.size", "Amount of queue in state ready"),
    QUEUE_STATE_CONSUMING_SIZE("redisques.queue.state.consuming.size", "Amount of queue in state consuming"),
    QUEUE_CONSUMER_COUNT("redisques.queue.consumers", "Count of consumer registered and alive for a redisques instance"),
    QUEUE_CONSUMER_LIFE_CYCLE("redisques.queue.consumer.life.cycle", "Count of consumer registered and alive for a redisques instance"),
    REBALANCE_MOVE("redisques.rebalance.moves", "Count of queue rebalance moves handled by rebalance requests, by result"),
    REBALANCE_CLAIM_RECOVERY("redisques.rebalance.claim.recovery", "Count of stale pending rebalance claims recovered by the claiming consumer, by result"),
    REBALANCE_QUEUE_RELEASED("redisques.rebalance.queues.released", "Count of queues this node gave up to another node through rebalancing"),
    REBALANCE_QUEUE_ACTIVATED("redisques.rebalance.queues.activated", "Count of queues this node started consuming after receiving them through rebalancing"),
    REBALANCE_QUEUE_ABANDONED("redisques.rebalance.queues.abandoned", "Count of rebalance claims this node rolled back to the previous owner");
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
