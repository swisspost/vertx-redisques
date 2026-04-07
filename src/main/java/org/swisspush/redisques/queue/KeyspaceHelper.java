package org.swisspush.redisques.queue;

import org.swisspush.redisques.util.RedisquesConfiguration;

public class KeyspaceHelper {
    private final RedisquesConfiguration configuration;
    private final String queuesKey;
    private final String queuesPrefix;
    private final String consumersPrefix;
    private final String clusterSafeConsumersPrefix;
    private final String dequeueStatisticKey;
    private final String dequeueStatisticTsKey;
    private final String locksKey;
    private final String queueCheckLastexecKey;
    private final String dataMigrationKey;
    private final String dataMigrationLockKey;

    private final String verticleUid;
    private final String verticleNotifyConsumerKey;
    private final String trimRequestKey;
    private final String consumersAddress;
    private final String aliveConsumersKey;
    private final String metricsCollectorAddress;
    public static final String QUEUE_STATE_COUNT_KEY = "queueStateCount";

    public KeyspaceHelper(RedisquesConfiguration configuration, String verticleUid) {
        this.configuration = configuration;
        this.verticleUid = verticleUid;
        queuesKey = configuration.getRedisPrefix() + "queues";
        queuesPrefix = configuration.getRedisPrefix() + "queues:";
        consumersPrefix = configuration.getRedisPrefix() + "consumers:";
        clusterSafeConsumersPrefix = configuration.getRedisPrefix() + "{consumers}:";
        dequeueStatisticKey =  configuration.getRedisPrefix() + "dequeueStatistic";
        dequeueStatisticTsKey =  dequeueStatisticKey + ":ts";
        locksKey = configuration.getRedisPrefix() + "locks";
        queueCheckLastexecKey = configuration.getRedisPrefix() + "check:lastexec";
        dataMigrationKey =  configuration.getRedisPrefix() + "migration";
        dataMigrationLockKey =  dataMigrationKey + ":locks";
        verticleNotifyConsumerKey = "notifyConsumer:" + verticleUid;
        trimRequestKey = "trim_request:" + verticleUid;
        consumersAddress = configuration.getAddress() + "-consumers";
        aliveConsumersKey= configuration.getRedisPrefix() + "-aliveConsumer";
        metricsCollectorAddress = configuration.getAddress()  + "-" + verticleUid + "-" + QUEUE_STATE_COUNT_KEY;
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

    public String getClusterSafeConsumersPrefix() {
        return clusterSafeConsumersPrefix;
    }

    public String getDequeueStatisticKey() {
        return dequeueStatisticKey;
    }

    public String getDequeueStatisticTsKey() {
        return dequeueStatisticTsKey;
    }

    public String getVerticleNotifyConsumerKey() {
        return verticleNotifyConsumerKey;
    }

    public String getTrimRequestKey() {
        return trimRequestKey;
    }

    public String getConsumersAddress() {
        return consumersAddress;
    }

    public String getAliveConsumersKey() {
        return aliveConsumersKey;
    }

    public  String getMetricsCollectorAddress() {
        return metricsCollectorAddress;
    }

    public String getDataMigrationLockKey() {
        return dataMigrationLockKey;
    }
}