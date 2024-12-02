package org.swisspush.redisques.util;

public enum MetricTags {

    IDENTIFIER("identifier");

    private final String id;

    MetricTags(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
