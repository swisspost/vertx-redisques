package org.swisspush.redisques.action;

import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.RedisquesConfiguration;

import static org.swisspush.redisques.util.RedisquesAPI.*;
import static org.swisspush.redisques.util.RedisquesAPI.LIMIT;

public class MonitorAction implements QueueAction {

    private final HttpClient client;
    private final RedisquesConfiguration modConfig;
    private final Logger log;

    public MonitorAction(RedisquesConfiguration modConfig, HttpClient client, Logger log) {
        this.modConfig = modConfig;
        this.client = client;
        this.log = log;
    }

    @Override
    public void execute(Message<JsonObject> event) {
        if(!modConfig.getHttpRequestHandlerEnabled()) {
            event.reply(createErrorReply().put(MESSAGE, "HttpRequestHandler is disabled"));
            return;
        }

        String limit = event.body().getJsonObject(PAYLOAD).getString(LIMIT);
        boolean emptyQueues = event.body().getJsonObject(PAYLOAD).getBoolean(EMPTY_QUEUES, false);

        String requestParams = "?limit=" + limit + "&emptyQueues=" + emptyQueues;

        client.request(HttpMethod.GET, modConfig.getHttpRequestHandlerPort(), "localhost",
                        modConfig.getHttpRequestHandlerPrefix()+ "/monitor" + requestParams)
            .compose(req -> req.send().compose(response -> {
                if (response.statusCode() == 200) {
                    return response.body();
                } else {
                    throw new RuntimeException("Failed to fetch monitor data: " + response.statusMessage());
                }
            }))
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    event.reply(createOkReply().put(VALUE, ar.result().toJsonObject()));
                } else {
                    event.reply(createErrorReply().put(MESSAGE, ar.cause().getMessage()));
                    log.warn("Failed to fetch monitor data", ar.cause());
                }
            });
    }
}
