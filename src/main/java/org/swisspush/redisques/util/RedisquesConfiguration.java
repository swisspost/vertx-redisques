package org.swisspush.redisques.util;

import com.google.common.base.Strings;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisClientType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class to configure the Redisques module.
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class RedisquesConfiguration {
    private final String address;
    private final String configurationUpdatedAddress;
    private final String redisPrefix;
    private final String processorAddress;
    private final String publishMetricsAddress;
    private final String metricStorageName;
    private final int metricRefreshPeriod;
    private final int refreshPeriod;
    private final List<String> redisHosts;
    private final List<Integer> redisPorts;
    private boolean redisEnableTls;
    private RedisClientType redisClientType;
    private final String redisAuth;
    private final String redisPassword;
    private final String redisUser;
    private final int checkInterval;
    private final int processorTimeout;
    private final long processorDelayMax;
    private final boolean httpRequestHandlerEnabled;
    private final boolean httpRequestHandlerAuthenticationEnabled;
    private final String httpRequestHandlerPrefix;
    private final String httpRequestHandlerUsername;
    private final String httpRequestHandlerPassword;
    private final Integer httpRequestHandlerPort;
    private final String httpRequestHandlerUserHeader;
    private final List<QueueConfiguration> queueConfigurations;
    private final boolean enableQueueNameDecoding;
    private final int maxPoolSize;
    private final int maxPoolWaitSize;
    private final int maxPipelineWaitSize;
    private final int queueSpeedIntervalSec;
    private final int memoryUsageLimitPercent;
    private final int memoryUsageCheckIntervalSec;
    private final int redisReadyCheckIntervalMs;
    private final int redisReconnectAttempts;
    private final int redisReconnectDelaySec;
    private final int redisPoolRecycleTimeoutMs;
    private final int dequeueStatisticReportIntervalSec;

    private static final int DEFAULT_CHECK_INTERVAL_S = 60; // 60s
    private static final int DEFAULT_PROCESSOR_TIMEOUT_MS = 240000; // 240s
    private static final int DEFAULT_METRIC_REFRESH_PERIOD_S = 10; // 10s
    private static final String DEFAULT_METRIC_STORAGE_NAME = "queue";
    private static final long DEFAULT_PROCESSOR_DELAY_MAX = 0;
    private static final int DEFAULT_REDIS_MAX_POOL_SIZE = 200;
    private static final int DEFAULT_REDIS_RECONNECT_ATTEMPTS = 0;
    private static final int DEFAULT_REDIS_RECONNECT_DELAY_SEC = 30;
    private static final int DEFAULT_REDIS_POOL_RECYCLE_TIMEOUT_MS = 180_000;

    // We want to have more than the default of 24 max waiting requests and therefore
    // set the default here to infinity value. See as well:
    // - https://groups.google.com/g/vertx/c/fe0RSWEfe8g
    // - https://vertx.io/docs/apidocs/io/vertx/redis/client/RedisOptions.html#setMaxPoolWaiting-int-
    // - https://stackoverflow.com/questions/59692663/vertx-java-httpclient-how-to-derive-maxpoolsize-and-maxwaitqueuesize-values-and
    private static final int DEFAULT_REDIS_MAX_POOL_WAIT_SIZE = Integer.MAX_VALUE;
    private static final int DEFAULT_REDIS_MAX_PIPELINE_WAIT_SIZE = 2048;
    private static final int DEFAULT_QUEUE_SPEED_INTERVAL_SEC = 60;
    private static final int DEFAULT_MEMORY_USAGE_LIMIT_PCT = 100;
    private static final int DEFAULT_MEMORY_USAGE_CHECK_INTERVAL_SEC = 60;
    private static final int DEFAULT_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC = -1;
    private static final int DEFAULT_REDIS_READY_CHECK_INTERVAL_MS = -1;

    public static final String PROP_ADDRESS = "address";
    public static final String PROP_CONFIGURATION_UPDATED_ADDRESS = "configuration-updated-address";
    public static final String PROP_REDIS_PREFIX = "redis-prefix";
    public static final String PROP_PROCESSOR_ADDRESS = "processor-address";
    public static final String PROP_PUBLISH_METRICS_ADDRESS = "publish-metrics-address";
    public static final String PROP_METRIC_STORAGE_NAME = "metric-storage-name";
    public static final String PROP_METRIC_REFRESH_PERIOD = "metric-refresh-period";
    public static final String PROP_REFRESH_PERIOD = "refresh-period";
    public static final String PROP_REDIS_HOST = "redisHost";
    public static final String PROP_REDIS_HOST_LIST = "redisHosts";
    public static final String PROP_REDIS_PORT = "redisPort";
    public static final String PROP_REDIS_PORT_LIST = "redisPorts";
    public static final String PROP_REDIS_CLIENT_TYPE = "redisClientType";
    public static final String PROP_REDIS_ENABLE_TLS = "redisEnableTls";
    /**
     * @deprecated Instance authentication is considered as legacy. With Redis from 6.x on the ACL authentication method should be used.
     */
    @Deprecated(since = "3.0.31")
    public static final String PROP_REDIS_AUTH = "redisAuth";
    public static final String PROP_REDIS_PASSWORD = "redisPassword";
    public static final String PROP_REDIS_USER = "redisUser";
    public static final String PROP_REDIS_RECONNECT_ATTEMPTS = "redisReconnectAttempts";
    public static final String PROP_REDIS_RECONNECT_DELAY_SEC = "redisReconnectDelaySec";
    public static final String PROP_REDIS_POOL_RECYCLE_TIMEOUT_MS = "redisPoolRecycleTimeoutMs";
    public static final String PROP_CHECK_INTERVAL = "checkInterval";
    public static final String PROP_PROCESSOR_TIMEOUT = "processorTimeout";
    public static final String PROP_PROCESSOR_DELAY_MAX = "processorDelayMax";
    public static final String PROP_HTTP_REQUEST_HANDLER_ENABLED = "httpRequestHandlerEnabled";
    public static final String PROP_HTTP_REQUEST_HANDLER_AUTH_ENABLED = "httpRequestHandlerAuthenticationEnabled";
    public static final String PROP_HTTP_REQUEST_HANDLER_PREFIX = "httpRequestHandlerPrefix";
    public static final String PROP_HTTP_REQUEST_HANDLER_USERNAME = "httpRequestHandlerUsername";
    public static final String PROP_HTTP_REQUEST_HANDLER_PASSWORD = "httpRequestHandlerPassword";
    public static final String PROP_HTTP_REQUEST_HANDLER_PORT = "httpRequestHandlerPort";
    public static final String PROP_HTTP_REQUEST_HANDLER_USER_HEADER = "httpRequestHandlerUserHeader";
    public static final String PROP_QUEUE_CONFIGURATIONS = "queueConfigurations";
    public static final String PROP_ENABLE_QUEUE_NAME_DECODING = "enableQueueNameDecoding";
    public static final String PROP_REDIS_MAX_POOL_SIZE = "maxPoolSize";
    public static final String PROP_REDIS_MAX_POOL_WAITING_SIZE = "maxPoolWaitingSize";
    public static final String PROP_REDIS_MAX_PIPELINE_WAITING_SIZE = "maxPipelineWaitingSize";
    public static final String PROP_QUEUE_SPEED_INTERVAL_SEC = "queueSpeedIntervalSec";
    public static final String PROP_MEMORY_USAGE_LIMIT_PCT = "memoryUsageLimitPercent";
    public static final String PROP_MEMORY_USAGE_CHECK_INTERVAL_SEC = "memoryUsageCheckIntervalSec";
    public static final String PROP_REDIS_READY_CHECK_INTERVAL_MS = "redisReadyCheckIntervalMs";
    public static final String PROP_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC = "dequeueStatisticReportIntervalSec";

    /**
     * Constructor with default values. Use the {@link RedisquesConfigurationBuilder} class
     * for simplified custom configuration.
     */
    public RedisquesConfiguration() {
        this(new RedisquesConfigurationBuilder());
    }

    /**
     * Constructor with redisAuth. Since Redis 6.x the use of redisAuth is considered as legacy.
     *
     * @deprecated
     */
    @Deprecated(since = "3.0.31")
    public RedisquesConfiguration(String address, String configurationUpdatedAddress, String redisPrefix, String processorAddress,
                                  String publishMetricsAddress, String metricStorageName, int metricRefreshPeriod, int refreshPeriod,
                                  String redisHost, int redisPort, String redisAuth, int checkInterval,
                                  int processorTimeout, long processorDelayMax, boolean httpRequestHandlerEnabled,
                                  boolean httpRequestHandlerAuthenticationEnabled, String httpRequestHandlerPrefix,
                                  String httpRequestHandlerUsername, String httpRequestHandlerPassword,
                                  Integer httpRequestHandlerPort, String httpRequestHandlerUserHeader,
                                  List<QueueConfiguration> queueConfigurations, boolean enableQueueNameDecoding) {
        this(address, configurationUpdatedAddress, redisPrefix, processorAddress, publishMetricsAddress, metricStorageName,
                metricRefreshPeriod, refreshPeriod, Collections.singletonList(redisHost), Collections.singletonList(redisPort),
                RedisClientType.STANDALONE, redisAuth, null, null, false, checkInterval,
                processorTimeout, processorDelayMax, httpRequestHandlerEnabled,
                httpRequestHandlerAuthenticationEnabled, httpRequestHandlerPrefix, httpRequestHandlerUsername,
                httpRequestHandlerPassword, httpRequestHandlerPort, httpRequestHandlerUserHeader, queueConfigurations,
                enableQueueNameDecoding, DEFAULT_REDIS_MAX_POOL_SIZE, DEFAULT_REDIS_MAX_POOL_WAIT_SIZE,
                DEFAULT_REDIS_MAX_PIPELINE_WAIT_SIZE, DEFAULT_QUEUE_SPEED_INTERVAL_SEC, DEFAULT_MEMORY_USAGE_LIMIT_PCT,
                DEFAULT_MEMORY_USAGE_CHECK_INTERVAL_SEC, DEFAULT_REDIS_RECONNECT_ATTEMPTS, DEFAULT_REDIS_RECONNECT_DELAY_SEC,
                DEFAULT_REDIS_POOL_RECYCLE_TIMEOUT_MS, DEFAULT_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC,
                DEFAULT_REDIS_READY_CHECK_INTERVAL_MS);
    }

    /**
     * Constructor with username and password (Redis ACL)
     * The parameter redisClustered is false
     */
    public RedisquesConfiguration(String address, String configurationUpdatedAddress, String redisPrefix, String processorAddress,
                                  String publishMetricsAddress, String metricStorageName, int metricRefreshPeriod, int refreshPeriod,
                                  String redisHost, int redisPort, String redisPassword, String redisUser, boolean redisEnableTls,
                                  int checkInterval, int processorTimeout, long processorDelayMax, boolean httpRequestHandlerEnabled,
                                  boolean httpRequestHandlerAuthenticationEnabled, String httpRequestHandlerPrefix,
                                  String httpRequestHandlerUsername, String httpRequestHandlerPassword,
                                  Integer httpRequestHandlerPort, String httpRequestHandlerUserHeader,
                                  List<QueueConfiguration> queueConfigurations, boolean enableQueueNameDecoding) {
        this(address, configurationUpdatedAddress, redisPrefix, processorAddress, publishMetricsAddress, metricStorageName,
                metricRefreshPeriod, refreshPeriod, Collections.singletonList(redisHost), Collections.singletonList(redisPort),
                RedisClientType.STANDALONE, null, redisPassword, redisUser, redisEnableTls, checkInterval,
                processorTimeout, processorDelayMax, httpRequestHandlerEnabled,
                httpRequestHandlerAuthenticationEnabled, httpRequestHandlerPrefix, httpRequestHandlerUsername,
                httpRequestHandlerPassword, httpRequestHandlerPort, httpRequestHandlerUserHeader, queueConfigurations,
                enableQueueNameDecoding, DEFAULT_REDIS_MAX_POOL_SIZE, DEFAULT_REDIS_MAX_POOL_WAIT_SIZE,
                DEFAULT_REDIS_MAX_PIPELINE_WAIT_SIZE, DEFAULT_QUEUE_SPEED_INTERVAL_SEC, DEFAULT_MEMORY_USAGE_LIMIT_PCT,
                DEFAULT_MEMORY_USAGE_CHECK_INTERVAL_SEC, DEFAULT_REDIS_RECONNECT_ATTEMPTS, DEFAULT_REDIS_RECONNECT_DELAY_SEC,
                DEFAULT_REDIS_POOL_RECYCLE_TIMEOUT_MS, DEFAULT_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC,
                DEFAULT_REDIS_READY_CHECK_INTERVAL_MS);
    }

    /**
     * Constructor with username and password (Redis ACL)
     */
    public RedisquesConfiguration(String address, String configurationUpdatedAddress, String redisPrefix, String processorAddress,
                                  String publishMetricsAddress, String metricStorageName, int metricRefreshPeriod, int refreshPeriod,
                                  String redisHost, int redisPort, RedisClientType redisClientType, String redisPassword,
                                  String redisUser, boolean redisEnableTls, int checkInterval,
                                  int processorTimeout, long processorDelayMax, boolean httpRequestHandlerEnabled,
                                  boolean httpRequestHandlerAuthenticationEnabled, String httpRequestHandlerPrefix,
                                  String httpRequestHandlerUsername, String httpRequestHandlerPassword,
                                  Integer httpRequestHandlerPort, String httpRequestHandlerUserHeader,
                                  List<QueueConfiguration> queueConfigurations, boolean enableQueueNameDecoding) {
        this(address, configurationUpdatedAddress, redisPrefix, processorAddress, publishMetricsAddress, metricStorageName,
                metricRefreshPeriod, refreshPeriod, Collections.singletonList(redisHost), Collections.singletonList(redisPort),
                redisClientType, null, redisPassword, redisUser, redisEnableTls, checkInterval, processorTimeout,
                processorDelayMax, httpRequestHandlerEnabled,
                httpRequestHandlerAuthenticationEnabled, httpRequestHandlerPrefix, httpRequestHandlerUsername,
                httpRequestHandlerPassword, httpRequestHandlerPort, httpRequestHandlerUserHeader, queueConfigurations,
                enableQueueNameDecoding, DEFAULT_REDIS_MAX_POOL_SIZE, DEFAULT_REDIS_MAX_POOL_WAIT_SIZE,
                DEFAULT_REDIS_MAX_PIPELINE_WAIT_SIZE, DEFAULT_QUEUE_SPEED_INTERVAL_SEC, DEFAULT_MEMORY_USAGE_LIMIT_PCT,
                DEFAULT_MEMORY_USAGE_CHECK_INTERVAL_SEC, DEFAULT_REDIS_RECONNECT_ATTEMPTS, DEFAULT_REDIS_RECONNECT_DELAY_SEC,
                DEFAULT_REDIS_POOL_RECYCLE_TIMEOUT_MS, DEFAULT_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC,
                DEFAULT_REDIS_READY_CHECK_INTERVAL_MS);
    }

    private RedisquesConfiguration(String address, String configurationUpdatedAddress, String redisPrefix, String processorAddress,
                                   String publishMetricsAddress, String metricStorageName, int metricRefreshPeriod, int refreshPeriod,
                                   List<String> redisHosts, List<Integer> redisPorts, RedisClientType redisClientType,
                                   String redisAuth, String redisPassword, String redisUser, boolean redisEnableTls, int checkInterval,
                                   int processorTimeout, long processorDelayMax, boolean httpRequestHandlerEnabled,
                                   boolean httpRequestHandlerAuthenticationEnabled, String httpRequestHandlerPrefix,
                                   String httpRequestHandlerUsername, String httpRequestHandlerPassword,
                                   Integer httpRequestHandlerPort, String httpRequestHandlerUserHeader,
                                   List<QueueConfiguration> queueConfigurations, boolean enableQueueNameDecoding,
                                   int maxPoolSize, int maxPoolWaitSize, int maxPipelineWaitSize,
                                   int queueSpeedIntervalSec, int memoryUsageLimitPercent, int memoryUsageCheckIntervalSec,
                                   int redisReconnectAttempts, int redisReconnectDelaySec, int redisPoolRecycleTimeoutMs,
                                   int dequeueStatisticReportIntervalSec, int redisReadyCheckIntervalMs) {
        this.address = address;
        this.configurationUpdatedAddress = configurationUpdatedAddress;
        this.redisPrefix = redisPrefix;
        this.processorAddress = processorAddress;
        this.publishMetricsAddress = publishMetricsAddress;
        this.refreshPeriod = refreshPeriod;
        this.redisHosts = redisHosts;
        this.redisPorts = redisPorts;
        this.redisClientType = redisClientType;
        this.redisAuth = redisAuth;
        this.redisPassword = redisPassword;
        this.redisUser = redisUser;
        this.redisEnableTls = redisEnableTls;
        this.maxPoolSize = maxPoolSize;
        this.maxPoolWaitSize = maxPoolWaitSize;
        this.maxPipelineWaitSize = maxPipelineWaitSize;
        Logger log = LoggerFactory.getLogger(RedisquesConfiguration.class);

        String maybeEmptyMetricStorageName = Strings.nullToEmpty(metricStorageName).trim();
        if(Strings.isNullOrEmpty(maybeEmptyMetricStorageName)) {
            this.metricStorageName = DEFAULT_METRIC_STORAGE_NAME;
        } else {
            this.metricStorageName = maybeEmptyMetricStorageName;
        }

        if (metricRefreshPeriod >= 1) {
            this.metricRefreshPeriod = metricRefreshPeriod;
        } else {
            log.warn("Overridden metricRefreshPeriod of {} is not valid. Using default value of {} instead.",
                    metricRefreshPeriod, DEFAULT_METRIC_REFRESH_PERIOD_S);
            this.metricRefreshPeriod = DEFAULT_METRIC_REFRESH_PERIOD_S;
        }

        if (checkInterval > 0) {
            this.checkInterval = checkInterval;
        } else {
            log.warn("Overridden checkInterval of {}s is not valid. Using default value of {}s instead.",
                    checkInterval, DEFAULT_CHECK_INTERVAL_S);
            this.checkInterval = DEFAULT_CHECK_INTERVAL_S;
        }

        if (processorTimeout >= 1) {
            this.processorTimeout = processorTimeout;
        } else {
            log.warn("Overridden processorTimeout of {} is not valid. Using default value of {} instead.",
                    processorTimeout, DEFAULT_PROCESSOR_TIMEOUT_MS);
            this.processorTimeout = DEFAULT_PROCESSOR_TIMEOUT_MS;
        }

        if (processorDelayMax >= 0) {
            this.processorDelayMax = processorDelayMax;
        } else {
            log.warn("Overridden processorDelayMax of {} is not valid. Using default value of {} instead.",
                    processorDelayMax, DEFAULT_PROCESSOR_DELAY_MAX);
            this.processorDelayMax = DEFAULT_PROCESSOR_DELAY_MAX;
        }

        this.httpRequestHandlerEnabled = httpRequestHandlerEnabled;
        this.httpRequestHandlerAuthenticationEnabled = httpRequestHandlerAuthenticationEnabled;
        this.httpRequestHandlerPrefix = httpRequestHandlerPrefix;
        this.httpRequestHandlerUsername = httpRequestHandlerUsername;
        this.httpRequestHandlerPassword = httpRequestHandlerPassword;
        this.httpRequestHandlerPort = httpRequestHandlerPort;
        this.httpRequestHandlerUserHeader = httpRequestHandlerUserHeader;
        this.queueConfigurations = queueConfigurations;
        this.enableQueueNameDecoding = enableQueueNameDecoding;
        this.queueSpeedIntervalSec = queueSpeedIntervalSec;

        if (memoryUsageCheckIntervalSec > 0) {
            this.memoryUsageCheckIntervalSec = memoryUsageCheckIntervalSec;
        } else {
            log.warn("Overridden memoryUsageCheckIntervalSec of {}s is not valid. Using default value of {}s instead.", memoryUsageCheckIntervalSec, DEFAULT_MEMORY_USAGE_CHECK_INTERVAL_SEC);
            this.memoryUsageCheckIntervalSec = DEFAULT_MEMORY_USAGE_CHECK_INTERVAL_SEC;
        }

        if (0 <= memoryUsageLimitPercent && memoryUsageLimitPercent <= 100) {
            this.memoryUsageLimitPercent = memoryUsageLimitPercent;
        } else {
            log.warn("Overridden memoryUsageLimitPercent of {} is not valid. Using default value of {} instead.", memoryUsageLimitPercent, DEFAULT_MEMORY_USAGE_LIMIT_PCT);
            this.memoryUsageLimitPercent = DEFAULT_MEMORY_USAGE_LIMIT_PCT;
        }

        this.redisReconnectAttempts = redisReconnectAttempts;

        if (redisReconnectDelaySec > 0) {
            this.redisReconnectDelaySec = redisReconnectDelaySec;
        } else {
            log.warn("Overridden redisReconnectDelaySec value of {} is not valid. Using value of 1 instead.", redisReconnectDelaySec);
            this.redisReconnectDelaySec = 1;
        }

        this.redisPoolRecycleTimeoutMs = redisPoolRecycleTimeoutMs;
        this.dequeueStatisticReportIntervalSec = dequeueStatisticReportIntervalSec;
        this.redisReadyCheckIntervalMs = redisReadyCheckIntervalMs;
    }

    public static RedisquesConfigurationBuilder with() {
        return new RedisquesConfigurationBuilder();
    }

    private RedisquesConfiguration(RedisquesConfigurationBuilder builder) {
        this(builder.address, builder.configurationUpdatedAddress, builder.redisPrefix,
                builder.processorAddress, builder.publishMetricsAddress, builder.metricStorageName, builder.metricRefreshPeriod,
                builder.refreshPeriod, builder.redisHosts, builder.redisPorts, builder.redisClientType, builder.redisAuth,
                builder.redisPassword, builder.redisUser, builder.redisEnableTls, builder.checkInterval,
                builder.processorTimeout, builder.processorDelayMax, builder.httpRequestHandlerEnabled,
                builder.httpRequestHandlerAuthenticationEnabled, builder.httpRequestHandlerPrefix,
                builder.httpRequestHandlerUsername, builder.httpRequestHandlerPassword, builder.httpRequestHandlerPort,
                builder.httpRequestHandlerUserHeader, builder.queueConfigurations,
                builder.enableQueueNameDecoding,
                builder.maxPoolSize, builder.maxPoolWaitSize, builder.maxPipelineWaitSize,
                builder.queueSpeedIntervalSec,
                builder.memoryUsageLimitPercent,
                builder.memoryUsageCheckIntervalSec,
                builder.redisReconnectAttempts,
                builder.redisReconnectDelaySec,
                builder.redisPoolRecycleTimeoutMs,
                builder.dequeueStatisticReportIntervalSec,
                builder.redisReadyCheckIntervalMs);
    }

    public JsonObject asJsonObject() {
        JsonObject obj = new JsonObject();
        obj.put(PROP_ADDRESS, getAddress());
        obj.put(PROP_CONFIGURATION_UPDATED_ADDRESS, getConfigurationUpdatedAddress());
        obj.put(PROP_REDIS_PREFIX, getRedisPrefix());
        obj.put(PROP_PROCESSOR_ADDRESS, getProcessorAddress());
        obj.put(PROP_PUBLISH_METRICS_ADDRESS, getPublishMetricsAddress());
        obj.put(PROP_METRIC_STORAGE_NAME, getMetricStorageName());
        obj.put(PROP_METRIC_REFRESH_PERIOD, getMetricRefreshPeriod());
        obj.put(PROP_REFRESH_PERIOD, getRefreshPeriod());
        obj.put(PROP_REDIS_HOST, getRedisHost());
        obj.put(PROP_REDIS_HOST_LIST, getRedisHosts());
        obj.put(PROP_REDIS_PORT, getRedisPort());
        obj.put(PROP_REDIS_PORT_LIST, getRedisPorts());
        obj.put(PROP_REDIS_RECONNECT_ATTEMPTS, getRedisReconnectAttempts());
        obj.put(PROP_REDIS_RECONNECT_DELAY_SEC, getRedisReconnectDelaySec());
        obj.put(PROP_REDIS_POOL_RECYCLE_TIMEOUT_MS, getRedisPoolRecycleTimeoutMs());
        obj.put(PROP_REDIS_AUTH, getRedisAuth());
        obj.put(PROP_REDIS_CLIENT_TYPE, getRedisClientType().name());
        obj.put(PROP_REDIS_PASSWORD, getRedisPassword());
        obj.put(PROP_REDIS_USER, getRedisUser());
        obj.put(PROP_REDIS_ENABLE_TLS, getRedisEnableTls());
        obj.put(PROP_CHECK_INTERVAL, getCheckInterval());
        obj.put(PROP_PROCESSOR_TIMEOUT, getProcessorTimeout());
        obj.put(PROP_PROCESSOR_DELAY_MAX, getProcessorDelayMax());
        obj.put(PROP_HTTP_REQUEST_HANDLER_ENABLED, getHttpRequestHandlerEnabled());
        obj.put(PROP_HTTP_REQUEST_HANDLER_AUTH_ENABLED, getHttpRequestHandlerAuthenticationEnabled());
        obj.put(PROP_HTTP_REQUEST_HANDLER_PREFIX, getHttpRequestHandlerPrefix());
        obj.put(PROP_HTTP_REQUEST_HANDLER_USERNAME, getHttpRequestHandlerUsername());
        obj.put(PROP_HTTP_REQUEST_HANDLER_PASSWORD, getHttpRequestHandlerPassword());
        obj.put(PROP_HTTP_REQUEST_HANDLER_PORT, getHttpRequestHandlerPort());
        obj.put(PROP_HTTP_REQUEST_HANDLER_USER_HEADER, getHttpRequestHandlerUserHeader());
        obj.put(PROP_QUEUE_CONFIGURATIONS, new JsonArray(getQueueConfigurations().stream().map(QueueConfiguration::asJsonObject).collect(Collectors.toList())));
        obj.put(PROP_ENABLE_QUEUE_NAME_DECODING, getEnableQueueNameDecoding());
        obj.put(PROP_REDIS_MAX_POOL_SIZE, getMaxPoolSize());
        obj.put(PROP_REDIS_MAX_POOL_WAITING_SIZE, getMaxPoolWaitSize());
        obj.put(PROP_REDIS_MAX_PIPELINE_WAITING_SIZE, getMaxPipelineWaitSize());
        obj.put(PROP_QUEUE_SPEED_INTERVAL_SEC, getQueueSpeedIntervalSec());
        obj.put(PROP_MEMORY_USAGE_LIMIT_PCT, getMemoryUsageLimitPercent());
        obj.put(PROP_MEMORY_USAGE_CHECK_INTERVAL_SEC, getMemoryUsageCheckIntervalSec());
        obj.put(PROP_REDIS_READY_CHECK_INTERVAL_MS, getRedisReadyCheckIntervalMs());
        obj.put(PROP_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC, getDequeueStatisticReportIntervalSec());
        return obj;
    }

    public static RedisquesConfiguration fromJsonObject(JsonObject json) {
        RedisquesConfigurationBuilder builder = RedisquesConfiguration.with();
        if (json.containsKey(PROP_ADDRESS)) {
            builder.address(json.getString(PROP_ADDRESS));
        }
        if (json.containsKey(PROP_CONFIGURATION_UPDATED_ADDRESS)) {
            builder.configurationUpdatedAddress(json.getString(PROP_CONFIGURATION_UPDATED_ADDRESS));
        }
        if (json.containsKey(PROP_REDIS_PREFIX)) {
            builder.redisPrefix(json.getString(PROP_REDIS_PREFIX));
        }
        if (json.containsKey(PROP_PROCESSOR_ADDRESS)) {
            builder.processorAddress(json.getString(PROP_PROCESSOR_ADDRESS));
        }
        if (json.containsKey(PROP_PUBLISH_METRICS_ADDRESS)) {
            builder.publishMetricsAddress(json.getString(PROP_PUBLISH_METRICS_ADDRESS));
        }
        if (json.containsKey(PROP_METRIC_REFRESH_PERIOD)) {
            builder.metricRefreshPeriod(json.getInteger(PROP_METRIC_REFRESH_PERIOD));
        }
        if (json.containsKey(PROP_METRIC_STORAGE_NAME)) {
            builder.metricStorageName(json.getString(PROP_METRIC_STORAGE_NAME));
        }
        if (json.containsKey(PROP_REFRESH_PERIOD)) {
            builder.refreshPeriod(json.getInteger(PROP_REFRESH_PERIOD));
        }
        if (json.containsKey(PROP_REDIS_HOST)) {
            builder.redisHost(json.getString(PROP_REDIS_HOST));
        }
        if (json.containsKey(PROP_REDIS_HOST_LIST)) {
            builder.redisHosts(json.getJsonArray(PROP_REDIS_HOST_LIST).getList());
        }
        if (json.containsKey(PROP_REDIS_PORT)) {
            builder.redisPort(json.getInteger(PROP_REDIS_PORT));
        }
        if (json.containsKey(PROP_REDIS_PORT_LIST)) {
            builder.redisPorts(json.getJsonArray(PROP_REDIS_PORT_LIST).getList());
        }
        if (json.containsKey(PROP_REDIS_RECONNECT_ATTEMPTS)) {
            builder.redisReconnectAttempts(json.getInteger(PROP_REDIS_RECONNECT_ATTEMPTS));
        }
        if (json.containsKey(PROP_REDIS_RECONNECT_DELAY_SEC)) {
            builder.redisReconnectDelaySec(json.getInteger(PROP_REDIS_RECONNECT_DELAY_SEC));
        }
        if (json.containsKey(PROP_REDIS_POOL_RECYCLE_TIMEOUT_MS)) {
            builder.redisPoolRecycleTimeoutMs(json.getInteger(PROP_REDIS_POOL_RECYCLE_TIMEOUT_MS));
        }
        if (json.containsKey(PROP_REDIS_CLIENT_TYPE)) {
            builder.redisClientType(RedisClientType.valueOf(json.getString(PROP_REDIS_CLIENT_TYPE)));
        }
        if (json.containsKey(PROP_REDIS_AUTH)) {
            builder.redisAuth(json.getString(PROP_REDIS_AUTH));
        }
        if (json.containsKey(PROP_REDIS_PASSWORD)) {
            builder.redisPassword(json.getString(PROP_REDIS_PASSWORD));
        }
        if (json.containsKey(PROP_REDIS_USER)) {
            builder.redisUser(json.getString(PROP_REDIS_USER));
        }
        if (json.containsKey(PROP_REDIS_ENABLE_TLS)) {
            builder.redisEnableTls(json.getBoolean(PROP_REDIS_ENABLE_TLS));
        }
        if (json.containsKey(PROP_CHECK_INTERVAL)) {
            builder.checkInterval(json.getInteger(PROP_CHECK_INTERVAL));
        }
        if (json.containsKey(PROP_PROCESSOR_TIMEOUT)) {
            builder.processorTimeout(json.getInteger(PROP_PROCESSOR_TIMEOUT));
        }
        if (json.containsKey(PROP_PROCESSOR_DELAY_MAX)) {
            builder.processorDelayMax(json.getLong(PROP_PROCESSOR_DELAY_MAX));
        }
        if (json.containsKey(PROP_HTTP_REQUEST_HANDLER_ENABLED)) {
            builder.httpRequestHandlerEnabled(json.getBoolean(PROP_HTTP_REQUEST_HANDLER_ENABLED));
        }
        if (json.containsKey(PROP_HTTP_REQUEST_HANDLER_AUTH_ENABLED)) {
            builder.httpRequestHandlerAuthenticationEnabled(json.getBoolean(PROP_HTTP_REQUEST_HANDLER_AUTH_ENABLED));
        }
        if (json.containsKey(PROP_HTTP_REQUEST_HANDLER_PREFIX)) {
            builder.httpRequestHandlerPrefix(json.getString(PROP_HTTP_REQUEST_HANDLER_PREFIX));
        }
        if (json.containsKey(PROP_HTTP_REQUEST_HANDLER_USERNAME)) {
            builder.httpRequestHandlerUsername(json.getString(PROP_HTTP_REQUEST_HANDLER_USERNAME));
        }
        if (json.containsKey(PROP_HTTP_REQUEST_HANDLER_PASSWORD)) {
            builder.httpRequestHandlerPassword(json.getString(PROP_HTTP_REQUEST_HANDLER_PASSWORD));
        }
        if (json.containsKey(PROP_HTTP_REQUEST_HANDLER_PORT)) {
            builder.httpRequestHandlerPort(json.getInteger(PROP_HTTP_REQUEST_HANDLER_PORT));
        }
        if (json.containsKey(PROP_HTTP_REQUEST_HANDLER_USER_HEADER)) {
            builder.httpRequestHandlerUserHeader(json.getString(PROP_HTTP_REQUEST_HANDLER_USER_HEADER));
        }
        if (json.containsKey(PROP_QUEUE_CONFIGURATIONS)) {
            builder.queueConfigurations((List<QueueConfiguration>) json.getJsonArray(PROP_QUEUE_CONFIGURATIONS)
                    .getList().stream()
                    .map(jsonObject -> QueueConfiguration.fromJsonObject((JsonObject) jsonObject))
                    .collect(Collectors.toList()));
        }
        if (json.containsKey(PROP_ENABLE_QUEUE_NAME_DECODING)) {
            builder.enableQueueNameDecoding(json.getBoolean(PROP_ENABLE_QUEUE_NAME_DECODING));
        }
        if (json.containsKey(PROP_REDIS_MAX_POOL_SIZE)) {
            builder.maxPoolSize(json.getInteger(PROP_REDIS_MAX_POOL_SIZE));
        }
        if (json.containsKey(PROP_REDIS_MAX_POOL_WAITING_SIZE)) {
            builder.maxPoolWaitSize(json.getInteger(PROP_REDIS_MAX_POOL_WAITING_SIZE));
        }
        if (json.containsKey(PROP_REDIS_MAX_PIPELINE_WAITING_SIZE)) {
            builder.maxPipelineWaitSize(json.getInteger(PROP_REDIS_MAX_PIPELINE_WAITING_SIZE));
        }
        if (json.containsKey(PROP_QUEUE_SPEED_INTERVAL_SEC)) {
            builder.queueSpeedIntervalSec(json.getInteger(PROP_QUEUE_SPEED_INTERVAL_SEC));
        }
        if (json.containsKey(PROP_MEMORY_USAGE_LIMIT_PCT)) {
            builder.memoryUsageLimitPercent(json.getInteger(PROP_MEMORY_USAGE_LIMIT_PCT));
        }
        if (json.containsKey(PROP_MEMORY_USAGE_CHECK_INTERVAL_SEC)) {
            builder.memoryUsageCheckIntervalSec(json.getInteger(PROP_MEMORY_USAGE_CHECK_INTERVAL_SEC));
        }
        if (json.containsKey(PROP_REDIS_READY_CHECK_INTERVAL_MS)) {
            builder.redisReadyCheckIntervalMs(json.getInteger(PROP_REDIS_READY_CHECK_INTERVAL_MS));
        }
        if (json.containsKey(PROP_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC)) {
            builder.dequeueStatisticReportIntervalSec(json.getInteger(PROP_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC));
        }
        return builder.build();
    }

    public String getAddress() {
        return address;
    }

    public String getConfigurationUpdatedAddress() {
        return configurationUpdatedAddress;
    }

    public String getRedisPrefix() {
        return redisPrefix;
    }

    public String getProcessorAddress() {
        return processorAddress;
    }

    public String getPublishMetricsAddress() {
        return publishMetricsAddress;
    }

    public String getMetricStorageName() {
        return metricStorageName;
    }

    public int getMetricRefreshPeriod() {
        return metricRefreshPeriod;
    }

    public int getRefreshPeriod() {
        return refreshPeriod;
    }

    public String getRedisHost() {
        return redisHosts.get(0);
    }

    public List<String> getRedisHosts() {
        return redisHosts;
    }

    public int getRedisPort() {
        return redisPorts.get(0);
    }
    public List<Integer> getRedisPorts() {
        return redisPorts;
    }

    public int getRedisReconnectAttempts() {
        return redisReconnectAttempts;
    }

    public int getRedisReconnectDelaySec() {
        return redisReconnectDelaySec;
    }

    public int getRedisPoolRecycleTimeoutMs() {
        return redisPoolRecycleTimeoutMs;
    }
    public int getDequeueStatisticReportIntervalSec() {
        return dequeueStatisticReportIntervalSec;
    }

    public String getRedisAuth() {
        return redisAuth;
    }

    public RedisClientType getRedisClientType() {
        return redisClientType;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public String getRedisUser() {
        return redisUser;
    }

    public boolean getRedisEnableTls() {
        return redisEnableTls;
    }

    public int getCheckInterval() {
        return checkInterval;
    }

    public int getProcessorTimeout() {
        return processorTimeout;
    }

    public long getProcessorDelayMax() {
        return processorDelayMax;
    }

    public boolean getHttpRequestHandlerEnabled() {
        return httpRequestHandlerEnabled;
    }

    public boolean getHttpRequestHandlerAuthenticationEnabled() {
        return httpRequestHandlerAuthenticationEnabled;
    }

    public String getHttpRequestHandlerPrefix() {
        return httpRequestHandlerPrefix;
    }

    public String getHttpRequestHandlerUsername() {
        return httpRequestHandlerUsername;
    }

    public String getHttpRequestHandlerPassword() {
        return httpRequestHandlerPassword;
    }

    public Integer getHttpRequestHandlerPort() {
        return httpRequestHandlerPort;
    }

    public String getHttpRequestHandlerUserHeader() {
        return httpRequestHandlerUserHeader;
    }

    public List<QueueConfiguration> getQueueConfigurations() {
        return queueConfigurations;
    }

    public boolean getEnableQueueNameDecoding() {
        return enableQueueNameDecoding;
    }

    /**
     * Gets the value for the vertx periodic timer.
     * This value is half of {@link RedisquesConfiguration#getCheckInterval()} in ms plus an additional 500ms.
     *
     * @return the interval for the vertx periodic timer
     */
    public int getCheckIntervalTimerMs() {
        return ((checkInterval * 1000) / 2) + 500;
    }

    /**
     * See {@link io.vertx.redis.client.RedisOptions#setMaxPoolSize(int)}
     */
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * See {@link io.vertx.redis.client.RedisOptions#setMaxPoolWaiting(int)}
     */
    public int getMaxPoolWaitSize() {
        return maxPoolWaitSize;
    }

    /**
     * See {@link io.vertx.redis.client.RedisOptions#setMaxWaitingHandlers(int)}
     */
    public int getMaxPipelineWaitSize() {
        return maxPipelineWaitSize;
    }

    /**
     * Retrieves the interval time in seconds in which the queue speed is calculated
     *
     * @return The speed interval time in seconds
     */
    public int getQueueSpeedIntervalSec() {
        return queueSpeedIntervalSec;
    }

    public int getMemoryUsageLimitPercent() {
        return memoryUsageLimitPercent;
    }

    public int getMemoryUsageCheckIntervalSec() {
        return memoryUsageCheckIntervalSec;
    }

    public int getRedisReadyCheckIntervalMs() {
        return redisReadyCheckIntervalMs;
    }

    @Override
    public String toString() {
        return asJsonObject().toString();
    }

    public boolean isDequeueStatsEnabled() {
        return getDequeueStatisticReportIntervalSec() > 0;
    }

    /**
     * RedisquesConfigurationBuilder class for simplified configuration.
     *
     * <pre>Usage:</pre>
     * <pre>
     * RedisquesConfiguration config = RedisquesConfiguration.with()
     *      .redisHost("anotherhost")
     *      .redisPort(1234)
     *      .build();
     * </pre>
     */
    public static class RedisquesConfigurationBuilder {
        private String address;
        private String configurationUpdatedAddress;
        private String redisPrefix;
        private String processorAddress;
        private String publishMetricsAddress;
        private String metricStorageName;
        private int metricRefreshPeriod;
        private int refreshPeriod;
        private List<String> redisHosts;
        private List<Integer>  redisPorts;
        private boolean redisEnableTls;
        private int redisReconnectAttempts;
        private int redisReconnectDelaySec;
        private int redisPoolRecycleTimeoutMs;
        private int dequeueStatisticReportIntervalSec;
        private RedisClientType redisClientType;
        private String redisAuth;
        private String redisPassword;
        private String redisUser;
        private int checkInterval;
        private int processorTimeout;
        private long processorDelayMax;
        private boolean httpRequestHandlerEnabled;
        private boolean httpRequestHandlerAuthenticationEnabled;
        private String httpRequestHandlerPrefix;
        private String httpRequestHandlerUsername;
        private String httpRequestHandlerPassword;
        private Integer httpRequestHandlerPort;
        private String httpRequestHandlerUserHeader;
        private List<QueueConfiguration> queueConfigurations;
        private boolean enableQueueNameDecoding;
        private int maxPoolSize;
        private int maxPoolWaitSize;
        private int maxPipelineWaitSize;
        private int queueSpeedIntervalSec;

        private int memoryUsageLimitPercent;
        private int memoryUsageCheckIntervalSec;
        private int redisReadyCheckIntervalMs;

        public RedisquesConfigurationBuilder() {
            this.address = "redisques";
            this.configurationUpdatedAddress = "redisques-configuration-updated";
            this.redisPrefix = "redisques:";
            this.processorAddress = "redisques-processor";
            this.metricRefreshPeriod = 10;
            this.refreshPeriod = 10;
            this.redisHosts = Collections.singletonList("localhost");
            this.redisPorts = Collections.singletonList(6379);
            this.redisEnableTls = false;
            this.redisClientType = RedisClientType.STANDALONE;
            this.redisReconnectAttempts = DEFAULT_REDIS_RECONNECT_ATTEMPTS;
            this.redisReconnectDelaySec = DEFAULT_REDIS_RECONNECT_DELAY_SEC;
            this.redisPoolRecycleTimeoutMs = DEFAULT_REDIS_POOL_RECYCLE_TIMEOUT_MS;
            this.checkInterval = DEFAULT_CHECK_INTERVAL_S; //60s
            this.processorTimeout = DEFAULT_PROCESSOR_TIMEOUT_MS;
            this.processorDelayMax = 0;
            this.httpRequestHandlerEnabled = false;
            this.httpRequestHandlerAuthenticationEnabled = false;
            this.httpRequestHandlerPrefix = "/queuing";
            this.httpRequestHandlerUsername = null;
            this.httpRequestHandlerPassword = null;
            this.httpRequestHandlerPort = 7070;
            this.httpRequestHandlerUserHeader = "x-rp-usr";
            this.queueConfigurations = new LinkedList<>();
            this.enableQueueNameDecoding = true;
            this.maxPoolSize = DEFAULT_REDIS_MAX_POOL_SIZE;
            this.maxPoolWaitSize = DEFAULT_REDIS_MAX_POOL_WAIT_SIZE;
            this.maxPipelineWaitSize = DEFAULT_REDIS_MAX_PIPELINE_WAIT_SIZE;
            this.queueSpeedIntervalSec = DEFAULT_QUEUE_SPEED_INTERVAL_SEC;
            this.memoryUsageLimitPercent = DEFAULT_MEMORY_USAGE_LIMIT_PCT;
            this.memoryUsageCheckIntervalSec = DEFAULT_MEMORY_USAGE_CHECK_INTERVAL_SEC;
            this.dequeueStatisticReportIntervalSec = DEFAULT_DEQUEUE_STATISTIC_REPORT_INTERVAL_SEC;
            this.redisReadyCheckIntervalMs = DEFAULT_REDIS_READY_CHECK_INTERVAL_MS;
        }

        public RedisquesConfigurationBuilder address(String address) {
            this.address = address;
            return this;
        }

        public RedisquesConfigurationBuilder configurationUpdatedAddress(String configurationUpdatedAddress) {
            this.configurationUpdatedAddress = configurationUpdatedAddress;
            return this;
        }

        public RedisquesConfigurationBuilder redisPrefix(String redisPrefix) {
            this.redisPrefix = redisPrefix;
            return this;
        }

        public RedisquesConfigurationBuilder processorAddress(String processorAddress) {
            this.processorAddress = processorAddress;
            return this;
        }

        public RedisquesConfigurationBuilder publishMetricsAddress(String publishMetricsAddress) {
            this.publishMetricsAddress = publishMetricsAddress;
            return this;
        }

        public RedisquesConfigurationBuilder metricStorageName(String metricStorageName) {
            this.metricStorageName = metricStorageName;
            return this;
        }

        public RedisquesConfigurationBuilder metricRefreshPeriod(int metricRefreshPeriod) {
            this.metricRefreshPeriod = metricRefreshPeriod;
            return this;
        }

        public RedisquesConfigurationBuilder refreshPeriod(int refreshPeriod) {
            this.refreshPeriod = refreshPeriod;
            return this;
        }

        public RedisquesConfigurationBuilder redisHost(String redisHost) {
            this.redisHosts = Collections.singletonList(redisHost);
            return this;
        }

        public RedisquesConfigurationBuilder redisHosts(List<String> redisHost) {
            this.redisHosts = redisHost;
            return this;
        }

        public RedisquesConfigurationBuilder redisPort(int redisPort) {
            this.redisPorts = Collections.singletonList(redisPort);
            return this;
        }

        public RedisquesConfigurationBuilder redisPorts(List<Integer> redisPorts) {
            this.redisPorts = redisPorts;
            return this;
        }

        public RedisquesConfigurationBuilder redisEnableTls(boolean redisEnableTls) {
            this.redisEnableTls = redisEnableTls;
            return this;
        }

        public RedisquesConfigurationBuilder redisReconnectAttempts(int redisReconnectAttempts) {
            this.redisReconnectAttempts = redisReconnectAttempts;
            return this;
        }

        public RedisquesConfigurationBuilder redisReconnectDelaySec(int redisReconnectDelaySec) {
            this.redisReconnectDelaySec = redisReconnectDelaySec;
            return this;
        }

        public RedisquesConfigurationBuilder redisPoolRecycleTimeoutMs(int redisPoolRecycleTimeoutMs) {
            this.redisPoolRecycleTimeoutMs = redisPoolRecycleTimeoutMs;
            return this;
        }

        public RedisquesConfigurationBuilder redisClientType(RedisClientType redisClientType) {
            this.redisClientType = redisClientType;
            return this;
        }

        public RedisquesConfigurationBuilder redisAuth(String redisAuth) {
            this.redisAuth = redisAuth;
            return this;
        }

        public RedisquesConfigurationBuilder redisPassword(String redisPassword) {
            this.redisPassword = redisPassword;
            return this;
        }

        public RedisquesConfigurationBuilder redisUser(String redisUser) {
            this.redisUser = redisUser;
            return this;
        }

        public RedisquesConfigurationBuilder checkInterval(int checkInterval) {
            this.checkInterval = checkInterval;
            return this;
        }

        public RedisquesConfigurationBuilder processorTimeout(int processorTimeout) {
            this.processorTimeout = processorTimeout;
            return this;
        }

        public RedisquesConfigurationBuilder processorDelayMax(long processorDelayMax) {
            this.processorDelayMax = processorDelayMax;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerEnabled(boolean httpRequestHandlerEnabled) {
            this.httpRequestHandlerEnabled = httpRequestHandlerEnabled;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerAuthenticationEnabled(boolean httpRequestHandlerAuthenticationEnabled) {
            this.httpRequestHandlerAuthenticationEnabled = httpRequestHandlerAuthenticationEnabled;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerPrefix(String httpRequestHandlerPrefix) {
            this.httpRequestHandlerPrefix = httpRequestHandlerPrefix;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerUsername(String httpRequestHandlerUsername) {
            this.httpRequestHandlerUsername = httpRequestHandlerUsername;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerPassword(String httpRequestHandlerPassword) {
            this.httpRequestHandlerPassword = httpRequestHandlerPassword;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerPort(Integer httpRequestHandlerPort) {
            this.httpRequestHandlerPort = httpRequestHandlerPort;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerUserHeader(String httpRequestHandlerUserHeader) {
            this.httpRequestHandlerUserHeader = httpRequestHandlerUserHeader;
            return this;
        }

        public RedisquesConfigurationBuilder queueConfigurations(List<QueueConfiguration> queueConfigurations) {
            this.queueConfigurations = queueConfigurations;
            return this;
        }

        public RedisquesConfigurationBuilder enableQueueNameDecoding(boolean enableQueueNameDecoding) {
            this.enableQueueNameDecoding = enableQueueNameDecoding;
            return this;
        }

        public RedisquesConfigurationBuilder maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        public RedisquesConfigurationBuilder maxPoolWaitSize(int maxWaitSize) {
            this.maxPoolWaitSize = maxWaitSize;
            return this;
        }

        public RedisquesConfigurationBuilder maxPipelineWaitSize(int maxWaitSize) {
            this.maxPipelineWaitSize = maxWaitSize;
            return this;
        }

        public RedisquesConfigurationBuilder queueSpeedIntervalSec(int queueSpeedIntervalSec) {
            this.queueSpeedIntervalSec = queueSpeedIntervalSec;
            return this;
        }

        public RedisquesConfigurationBuilder memoryUsageLimitPercent(int memoryUsageLimitPercent) {
            this.memoryUsageLimitPercent = memoryUsageLimitPercent;
            return this;
        }

        public RedisquesConfigurationBuilder memoryUsageCheckIntervalSec(int memoryUsageCheckIntervalSec) {
            this.memoryUsageCheckIntervalSec = memoryUsageCheckIntervalSec;
            return this;
        }

        public RedisquesConfigurationBuilder redisReadyCheckIntervalMs(int redisReadyCheckIntervalMs) {
            this.redisReadyCheckIntervalMs = redisReadyCheckIntervalMs;
            return this;
        }

        public RedisquesConfigurationBuilder dequeueStatisticReportIntervalSec(int dequeueStatisticReportIntervalSec) {
            this.dequeueStatisticReportIntervalSec = dequeueStatisticReportIntervalSec;
            return this;
        }
        public RedisquesConfiguration build() {
            return new RedisquesConfiguration(this);
        }
    }

}
