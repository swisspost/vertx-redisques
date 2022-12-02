package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.swisspush.redisques.util.QueueConfiguration;
import org.swisspush.redisques.util.RedisquesConfiguration;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.VALUE;

public class GetConfigurationAction implements QueueAction {

    private String address;
    private String redisPrefix;
    private String processorAddress;
    private String redisHost;
    private int redisPort;
    private String redisAuth;
    private String redisEncoding;
    private int refreshPeriod;
    private int redisMaxPoolSize;
    private int redisMaxPoolWaitingSize;
    private int redisMaxPipelineWaitingSize;
    private int checkInterval;
    private int processorTimeout;

    private long processorDelayMax;
    private boolean httpRequestHandlerEnabled;
    private String httpRequestHandlerPrefix;
    private int httpRequestHandlerPort;
    private String httpRequestHandlerUserHeader;
    private List<QueueConfiguration> queueConfigurations;

    public GetConfigurationAction(String address, String redisPrefix, String processorAddress, String redisHost,
                                  int redisPort, String redisAuth, String redisEncoding, int refreshPeriod,
                                  int redisMaxPoolSize, int redisMaxPoolWaitingSize, int redisMaxPipelineWaitingSize,
                                  int checkInterval, int processorTimeout, long processorDelayMax,
                                  boolean httpRequestHandlerEnabled, String httpRequestHandlerPrefix,
                                  int httpRequestHandlerPort, String httpRequestHandlerUserHeader,
                                  List<QueueConfiguration> queueConfigurations) {
        this.address = address;
        this.redisPrefix = redisPrefix;
        this.processorAddress = processorAddress;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisAuth = redisAuth;
        this.redisEncoding = redisEncoding;
        this.refreshPeriod = refreshPeriod;
        this.redisMaxPoolSize = redisMaxPoolSize;
        this.redisMaxPoolWaitingSize = redisMaxPoolWaitingSize;
        this.redisMaxPipelineWaitingSize = redisMaxPipelineWaitingSize;
        this.checkInterval = checkInterval;
        this.processorTimeout = processorTimeout;
        this.processorDelayMax = processorDelayMax;
        this.httpRequestHandlerEnabled = httpRequestHandlerEnabled;
        this.httpRequestHandlerPrefix = httpRequestHandlerPrefix;
        this.httpRequestHandlerPort = httpRequestHandlerPort;
        this.httpRequestHandlerUserHeader = httpRequestHandlerUserHeader;
        this.queueConfigurations = queueConfigurations;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        JsonObject result = new JsonObject();
        result.put(RedisquesConfiguration.PROP_ADDRESS, address);
        result.put(RedisquesConfiguration.PROP_REDIS_PREFIX, redisPrefix);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_ADDRESS, processorAddress);
        result.put(RedisquesConfiguration.PROP_REFRESH_PERIOD, refreshPeriod);
        result.put(RedisquesConfiguration.PROP_REDIS_HOST, redisHost);
        result.put(RedisquesConfiguration.PROP_REDIS_PORT, redisPort);
        result.put(RedisquesConfiguration.PROP_REDIS_AUTH, redisAuth);
        result.put(RedisquesConfiguration.PROP_REDIS_ENCODING, redisEncoding);
        result.put(RedisquesConfiguration.PROP_CHECK_INTERVAL, checkInterval);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_TIMEOUT, processorTimeout);
        result.put(RedisquesConfiguration.PROP_PROCESSOR_DELAY_MAX, processorDelayMax);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_ENABLED, httpRequestHandlerEnabled);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_PREFIX, httpRequestHandlerPrefix);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_PORT, httpRequestHandlerPort);
        result.put(RedisquesConfiguration.PROP_HTTP_REQUEST_HANDLER_USER_HEADER, httpRequestHandlerUserHeader);
        event.reply(QueueAction.createOkReply().put(VALUE, result));
    }
}
