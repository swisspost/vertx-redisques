package org.swisspush.redisques.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class QueueConfiguration {
    
    public static final String PROP_PATTERN = "pattern";
    public static final String PROP_RETRY_INTERVALS = "retryIntervals";
    
    private String pattern;
    
    private List<Integer> retryIntervals;

    public String getPattern() {
        return pattern;
    }

    public List<Integer> getRetryIntervals() {
        return retryIntervals;
    }

    public static QueueConfigurationBulider with() {
        return new QueueConfigurationBulider();
    }
    
    public JsonObject asJsonObject() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put(PROP_PATTERN, getPattern());
        jsonObject.put(PROP_RETRY_INTERVALS, new JsonArray(getRetryIntervals()));
        return jsonObject;
    }
    
    public static QueueConfiguration fromJsonObject(JsonObject jsonObject) {
        QueueConfigurationBulider builder = QueueConfiguration.with();
        if (jsonObject.containsKey(PROP_PATTERN)) {
            builder.pattern(jsonObject.getString(PROP_PATTERN));
        }
        if (jsonObject.containsKey(PROP_RETRY_INTERVALS)){
            builder.retryIntervals(jsonObject.getJsonArray(PROP_RETRY_INTERVALS).getList());
        }
        return builder.build();
    }

    private QueueConfiguration(QueueConfigurationBulider builder) {
        this.pattern = builder.pattern;
        this.retryIntervals = builder.retryIntervals;
    }
    
    public static class QueueConfigurationBulider {
        
        String pattern;
        List<Integer> retryIntervals;
        
        public QueueConfigurationBulider pattern(String pattern) {
            this.pattern = pattern;
            return this;
        }
        
        public QueueConfigurationBulider retryIntervals(List<Integer> retryIntervals) {
            this.retryIntervals = retryIntervals;
            return this;
        }
        
        public QueueConfiguration build() {
            return new QueueConfiguration(this);
        }
    }
}
