package org.swisspush.redisques.util;

public enum MetricTags {

    IDENTIFIER("identifier"),
    QUEUE_NAME("queue_name"),
    CONSUMER_UID("consumer_uid");

    private final String id;

    MetricTags(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
