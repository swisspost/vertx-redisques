package org.swisspush.redisques.queue;

import org.swisspush.redisques.util.RedisquesConfiguration;

public class KeyspaceHelper {
    private final RedisquesConfiguration configuration;
    private final String queuesKey;
    private final String queuesPrefix;
    private final String consumersPrefix;
    private final String locksKey;
    private final String queueCheckLastexecKey;

    private final String verticleUid;
    private final String verticleRefreshRegistrationKey;
    private final String verticleStartConsumeKey;
    private final String verticleNotifyConsumerKey;
    private final String trimRequestKey;
    public KeyspaceHelper(RedisquesConfiguration configuration, String verticleUid) {
        this.configuration = configuration;
        this.verticleUid = verticleUid;
        queuesKey = configuration.getRedisPrefix() + "queues";
        queuesPrefix = configuration.getRedisPrefix() + "queues:";
        consumersPrefix = configuration.getRedisPrefix() + "consumers:";
        locksKey = configuration.getRedisPrefix() + "locks";
        queueCheckLastexecKey = configuration.getRedisPrefix() + "check:lastexec";
        verticleRefreshRegistrationKey = "refreshRegistration:" + verticleUid;
        verticleStartConsumeKey = "startConsumer:" + verticleUid;
        verticleNotifyConsumerKey = "notifyConsumer:" + verticleUid;
        trimRequestKey = "trim_request:" + verticleUid;
    }

    public String getAddress() {
        return configuration.getAddress();
    }


    public String getVerticleUid() {
        return verticleUid;
    }

    public String getQueueCheckLastExecKey() {
        return queueCheckLastexecKey;
    }

    public String getLocksKey() {
        return locksKey;
    }

    public String getQueuesKey() {
        return queuesKey;
    }

    public String getQueuesPrefix() {
        return queuesPrefix;
    }

    public String getConsumersPrefix() {
        return consumersPrefix;
    }

    public String getVerticleRefreshRegistrationKey() {
        return verticleRefreshRegistrationKey;
    }

    public String getVerticleStartConsumeKey() {
        return verticleStartConsumeKey;
    }

    public String getVerticleNotifyConsumerKey() {
        return verticleNotifyConsumerKey;
    }

    public String getTrimRequestKey() {
        return trimRequestKey;
    }
}