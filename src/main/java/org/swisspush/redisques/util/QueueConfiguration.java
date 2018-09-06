package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;

import java.util.List;

public class QueueConfiguration {
    
    static final String PROP_PATTERN = "pattern";
    static final String PROP_RETRY_INTERVALS = "retryIntervals";
    
    private String pattern;
    
    private List<Integer> retryIntervals;

    @SuppressWarnings("unused")
    public QueueConfiguration() {
        // required to be deserialized by io.vertx.core.json.JsonObject.mapTo(type)
    }

    public String getPattern() {
        return pattern;
    }

    public List<Integer> getRetryIntervals() {
        return retryIntervals;
    }

    public static QueueConfigurationBuilder with() {
        return new QueueConfigurationBuilder();
    }
    
    public JsonObject asJsonObject() {
        return JsonObject.mapFrom(this);
    }
    
    static QueueConfiguration fromJsonObject(JsonObject jsonObject) {
        return jsonObject.mapTo(QueueConfiguration.class);
    }

    private QueueConfiguration(QueueConfigurationBuilder builder) {
        this.pattern = builder.pattern;
        this.retryIntervals = builder.retryIntervals;
    }
    
    public static class QueueConfigurationBuilder {
        
        String pattern;
        List<Integer> retryIntervals;
        
        public QueueConfigurationBuilder pattern(String pattern) {
            this.pattern = pattern;
            return this;
        }
        
        public QueueConfigurationBuilder retryIntervals(List<Integer> retryIntervals) {
            this.retryIntervals = retryIntervals;
            return this;
        }
        
        public QueueConfiguration build() {
            return new QueueConfiguration(this);
        }
    }
}
