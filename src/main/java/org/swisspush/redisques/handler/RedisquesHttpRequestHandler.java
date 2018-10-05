package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.Result;
import org.swisspush.redisques.util.StatusCode;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.swisspush.redisques.util.HttpServerRequestUtil.*;
import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Handler class for HTTP requests providing access to Redisques over HTTP.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesHttpRequestHandler implements Handler<HttpServerRequest> {


    private static Logger log = LoggerFactory.getLogger(RedisquesHttpRequestHandler.class);

    private Router router;
    private EventBus eventBus;

    private static final String APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE = "content-type";
    private static final String LOCKED_PARAM = "locked";
    private static final String UNLOCK_PARAM = "unlock";
    private static final String COUNT_PARAM = "count";
    private static final String BULK_DELETE_PARAM = "bulkDelete";
    private static final String EMPTY_QUEUES_PARAM = "emptyQueues";
    private static final String DELETED = "deleted";

    private final String redisquesAddress;
    private final String userHeader;

    public static void init(Vertx vertx, RedisquesConfiguration modConfig) {
        log.info("Enable http request handler: " + modConfig.getHttpRequestHandlerEnabled());
        if (modConfig.getHttpRequestHandlerEnabled()) {
            if (modConfig.getHttpRequestHandlerPort() != null && modConfig.getHttpRequestHandlerUserHeader() != null) {
                RedisquesHttpRequestHandler handler = new RedisquesHttpRequestHandler(vertx, modConfig);
                // in Vert.x 2x 100-continues was activated per default, in vert.x 3x it is off per default.
                HttpServerOptions options = new HttpServerOptions().setHandle100ContinueAutomatically(true);
                vertx.createHttpServer(options).requestHandler(handler).listen(modConfig.getHttpRequestHandlerPort(), result -> {
                    if (result.succeeded()) {
                        log.info("Successfully started http request handler on port " + modConfig.getHttpRequestHandlerPort());
                    } else {
                        log.error("Unable to start http request handler.", result.cause());
                    }
                });
            } else {
                log.error("Configured to enable http request handler but no port configuration and/or user header configuration provided");
            }
        }
    }

    private RedisquesHttpRequestHandler(Vertx vertx, RedisquesConfiguration modConfig) {
        this.router = Router.router(vertx);
        this.eventBus = vertx.eventBus();
        this.redisquesAddress = modConfig.getAddress();
        this.userHeader = modConfig.getHttpRequestHandlerUserHeader();

        final String prefix = modConfig.getHttpRequestHandlerPrefix();

        /*
         * List endpoints
         */
        router.get(prefix + "/").handler(this::listEndpoints);

        /*
         * Get configuration
         */
        router.get(prefix + "/configuration/").handler(this::getConfiguration);

        /*
         * Set configuration
         */
        router.post(prefix + "/configuration/").handler(this::setConfiguration);

        /*
         * Get monitor information
         */
        router.get(prefix + "/monitor/").handler(this::getMonitorInformation);

        /*
         * Enqueue or LockedEnqueue
         */
        router.putWithRegex(prefix + "/enqueue/([^/]+)/").handler(this::enqueueOrLockedEnqueue);

        /*
         * List queue items
         */
        router.getWithRegex(prefix + "/monitor/[^/]+").handler(this::listQueueItems);

        /*
         * List or count queues
         */
        router.get(prefix + "/queues/").handler(this::listOrCountQueues);

        /*
         * List or count queue items
         */
        router.getWithRegex(prefix + "/queues/[^/]+").handler(this::listOrCountQueueItems);

        /*
         * Delete all queue items
         */
        router.deleteWithRegex(prefix + "/queues/[^/]+").handler(this::deleteAllQueueItems);

        /*
         * Bulk delete queues
         */
        router.post(prefix + "/queues/").handler(this::bulkDeleteQueues);

        /*
         * Get single queue item
         */
        router.getWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::getSingleQueueItem);

        /*
         * Replace single queue item
         */
        router.putWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::replaceSingleQueueItem);

        /*
         * Delete single queue item
         */
        router.deleteWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::deleteQueueItem);

        /*
         * Add queue item
         */
        router.postWithRegex(prefix + "/queues/([^/]+)/").handler(this::addQueueItem);

        /*
         * Get all locks
         */
        router.get(prefix + "/locks/").handler(this::getAllLocks);

        /*
         * Add lock
         */
        router.putWithRegex(prefix + "/locks/[^/]+").handler(this::addLock);

        /*
         * Get single lock
         */
        router.getWithRegex(prefix + "/locks/[^/]+").handler(this::getSingleLock);

        /*
         * Delete all locks
         */
        router.delete(prefix + "/locks/").handler(this::deleteAllLocks);

        /*
         * Bulk create / delete locks
         */
        router.post(prefix + "/locks/").handler(this::bulkPutOrDeleteLocks);

        /*
         * Delete single lock
         */
        router.deleteWithRegex(prefix + "/locks/[^/]+").handler(this::deleteSingleLock);

        router.routeWithRegex(".*").handler(this::respondMethodNotAllowed);
    }

    @Override
    public void handle(HttpServerRequest request) {
        router.accept(request);
    }

    private void respondMethodNotAllowed(RoutingContext ctx) {
        respondWith(StatusCode.METHOD_NOT_ALLOWED, ctx.request());
    }

    private void listEndpoints(RoutingContext ctx) {
        JsonObject result = new JsonObject();
        JsonArray items = new JsonArray();
        items.add("locks/");
        items.add("queues/");
        items.add("monitor/");
        items.add("configuration/");
        result.put(lastPart(ctx.request().path()), items);
        ctx.response().putHeader(CONTENT_TYPE, APPLICATION_JSON);
        ctx.response().end(result.encode());
    }

    private void enqueueOrLockedEnqueue(RoutingContext ctx) {
        final String queue = part(ctx.request().path(), 1);
        ctx.request().bodyHandler(buffer -> {
            try {
                String strBuffer = encodePayload(buffer.toString());
                eventBus.send(redisquesAddress, buildEnqueueOrLockedEnqueueOperation(queue, strBuffer, ctx.request()),
                        (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                            if (reply.failed()) {
                                log.warn("Received failed message for enqueueOrLockedEnqueue. But will continue anyway. _441dcfdffcbf422e24_", reply.cause());
                                // We should respond with 'HTTP 5xx' here. But we don't, to keep backward compatibility.
                                // Why we should? See: "https://softwareengineering.stackexchange.com/a/190535"
                            }
                            checkReply(reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
                        }
                );
            } catch (Exception ex) {
                respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
            }
        });
    }

    private JsonObject buildEnqueueOrLockedEnqueueOperation(String queue, String message, HttpServerRequest request) {
        if (evaluateUrlParameterToBeEmptyOrTrue(LOCKED_PARAM, request)) {
            return buildLockedEnqueueOperation(queue, message, extractUser(request));
        } else {
            return buildEnqueueOperation(queue, message);
        }
    }

    private void getAllLocks(RoutingContext ctx) {
        String filter = ctx.request().params().get(FILTER);
        eventBus.send(redisquesAddress, buildGetAllLocksOperation(filter), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.failed()) {
                log.warn("Received failed message for getAllLocksOperation. Lets run into NullPointerException now. _b90a185fa905e_", reply.cause());
                // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
            }
            final JsonObject body = reply.result().body();
            if (OK.equals(body.getString(STATUS))) {
                jsonResponse(ctx.response(), body.getJsonObject(VALUE));
            } else {
                String errorType = body.getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                    if (body.getString(MESSAGE) != null) {
                        respondWith(StatusCode.BAD_REQUEST, body.getString(MESSAGE), ctx.request());
                    } else {
                        respondWith(StatusCode.BAD_REQUEST, ctx.request());
                    }
                } else {
                    respondWith(StatusCode.NOT_FOUND, ctx.request());
                }
            }
        });
    }

    private void addLock(RoutingContext ctx) {
        String queue = lastPart(ctx.request().path());
        eventBus.send(redisquesAddress, buildPutLockOperation(queue, extractUser(ctx.request())),
                (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                    if (reply.failed()) {
                        log.warn("Received failed message for addLockOperation. Lets run into NullPointerException now. _9dafe6dbc8ee87c8d0b1_", reply.cause());
                        // IMO we should respond with 'HTTP 5xx' here. But we don't, to keep backward compatibility.
                        // Nevertheless. Lets run into NullPointerException by calling method below.
                    }
                    checkReply(reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
                }
        );
    }

    private void getSingleLock(RoutingContext ctx) {
        String queue = lastPart(ctx.request().path());
        eventBus.send(redisquesAddress, buildGetLockOperation(queue), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            final HttpServerResponse response = ctx.response();
            if (reply.failed()) {
                log.warn("Received failed message for getSingleLockOperation. Lets run into NullPointerException now. _f82af1e092812d59cf_", reply.cause());
                // IMO we should respond with 'HTTP 5xx' here. But we don't, to keep backward compatibility.
            }
            if (OK.equals(reply.result().body().getString(STATUS))) {
                response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
                response.end(reply.result().body().getString(VALUE));
            } else {
                response.setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                response.setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                response.end(NO_SUCH_LOCK);
            }
        });
    }

    private void deleteSingleLock(RoutingContext ctx) {
        String queue = lastPart(ctx.request().path());
        eventBus.send(redisquesAddress, buildDeleteLockOperation(queue),
                (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                    if (reply.failed()) {
                        log.warn("Received failed message for deleteSingleLockOperation. Lets run into NullPointerException now. _5dcf418d606183_", reply.cause());
                        // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
                        // Nevertheless. Lets run into NullPointerException by calling below method.
                    }
                    checkReply(reply.result(), ctx.request(), StatusCode.INTERNAL_SERVER_ERROR);
                });
    }

    private void deleteAllLocks(RoutingContext ctx) {
        eventBus.send(redisquesAddress, buildDeleteAllLocksOperation(), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonObject result = new JsonObject();
                result.put(DELETED, reply.result().body().getLong(VALUE));
                jsonResponse(ctx.response(), result);
            } else {
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error deleting all locks", ctx.request());
            }
        });
    }

    private void bulkPutOrDeleteLocks(RoutingContext ctx) {
        ctx.request().bodyHandler(buffer -> {
            try {
                Result<JsonArray, String> result = extractNonEmptyJsonArrayFromBody(LOCKS, buffer.toString());
                if (result.isErr()) {
                    respondWith(StatusCode.BAD_REQUEST, result.getErr(), ctx.request());
                    return;
                }

                if (evaluateUrlParameterToBeEmptyOrTrue(BULK_DELETE_PARAM, ctx.request())) {
                    bulkDeleteLocks(ctx, result.getOk());
                } else {
                    bulkPutLocks(ctx, result.getOk());
                }
            } catch (Exception ex) {
                respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
            }
        });
    }

    private void bulkDeleteLocks(RoutingContext ctx, JsonArray locks) {
        eventBus.send(redisquesAddress, buildBulkDeleteLocksOperation(locks), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonObject result = new JsonObject();
                result.put(DELETED, reply.result().body().getLong(VALUE));
                jsonResponse(ctx.response(), result);
            } else {
                String errorType = reply.result().body().getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                    respondWith(StatusCode.BAD_REQUEST, reply.result().body().getString(MESSAGE), ctx.request());
                } else {
                    respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error bulk deleting locks", ctx.request());
                }
            }
        });
    }

    private void bulkPutLocks(RoutingContext ctx, JsonArray locks) {
        eventBus.send(redisquesAddress, buildBulkPutLocksOperation(locks, extractUser(ctx.request())),
                (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                    if (reply.failed()) {
                        log.warn("Problem while bulkPutLocks", reply.cause());
                        // Continue, only to keep backward compatibility.
                    }
                    if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                        respondWith(StatusCode.OK, ctx.request());
                    } else {
                        final JsonObject body = reply.result().body();
                        final String errorType = body.getString(ERROR_TYPE);
                        if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                            respondWith(StatusCode.BAD_REQUEST, body.getString(MESSAGE), ctx.request());
                        } else {
                            respondWith(StatusCode.INTERNAL_SERVER_ERROR, ctx.request());
                        }
                    }
                });
    }

    private void getQueueItemsCount(RoutingContext ctx) {
        final String queue = lastPart(ctx.request().path());
        eventBus.send(redisquesAddress, buildGetQueueItemsCountOperation(queue), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.failed()) {
                log.warn("Failed to getQueueItemsCount", reply.cause());
                // Continue, only to keep backward compatibility.
            }
            if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonObject result = new JsonObject();
                result.put("count", reply.result().body().getLong(VALUE));
                jsonResponse(ctx.response(), result);
            } else {
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error gathering count of active queue items", ctx.request());
            }
        });
    }

    private void getConfiguration(RoutingContext ctx) {
        eventBus.send(redisquesAddress, buildGetConfigurationOperation(), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.failed()) {
                log.warn("Failed to getConfiguration.", reply.cause());
                // Continue, only to keep backward compatibility.
            }
            if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                jsonResponse(ctx.response(), reply.result().body().getJsonObject(VALUE));
            } else {
                String error = "Error gathering configuration";
                log.error(error);
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, error, ctx.request());
            }
        });
    }

    private void setConfiguration(RoutingContext ctx) {
        ctx.request().bodyHandler((Buffer buffer) -> {
            try {
                JsonObject configurationValues = new JsonObject(buffer.toString());
                eventBus.send(redisquesAddress, buildSetConfigurationOperation(configurationValues),
                        (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                            if (reply.failed()) {
                                log.error("Failed to setConfiguration.", reply.cause());
                                respondWith(StatusCode.INTERNAL_SERVER_ERROR, reply.cause().getMessage(), ctx.request());
                            } else {
                                if (OK.equals(reply.result().body().getString(STATUS))) {
                                    respondWith(StatusCode.OK, ctx.request());
                                } else {
                                    respondWith(StatusCode.BAD_REQUEST, reply.result().body().getString(MESSAGE), ctx.request());
                                }
                            }
                        });
            } catch (Exception ex) {
                respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
            }
        });
    }

    private void getMonitorInformation(RoutingContext ctx) {
        boolean emptyQueues = evaluateUrlParameterToBeEmptyOrTrue(EMPTY_QUEUES_PARAM, ctx.request());
        final JsonObject resultObject = new JsonObject();
        final JsonArray queuesArray = new JsonArray();
        eventBus.send(redisquesAddress, buildGetQueuesOperation(), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.failed()) {
                log.warn("Failed to getMonitorInformation", reply.cause());
                // Continue, to keep backward compatibility (aka run into NPE).
            }
            if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                final List<String> queueNames = reply.result().body().getJsonObject(VALUE).getJsonArray("queues").getList();
                collectQueueLengths(queueNames, extractLimit(ctx), emptyQueues, mapEntries -> {
                    for (Map.Entry<String, Long> entry : mapEntries) {
                        JsonObject obj = new JsonObject();
                        obj.put("name", entry.getKey());
                        obj.put("size", entry.getValue());
                        queuesArray.add(obj);
                    }
                    resultObject.put("queues", queuesArray);
                    jsonResponse(ctx.response(), resultObject);
                });
            } else {
                String error = "Error gathering names of active queues";
                log.error(error);
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, error, ctx.request());
            }
        });
    }

    private void listOrCountQueues(RoutingContext ctx) {
        if (evaluateUrlParameterToBeEmptyOrTrue(COUNT_PARAM, ctx.request())) {
            getQueuesCount(ctx);
        } else {
            listQueues(ctx);
        }
    }

    private void getQueuesCount(RoutingContext ctx) {
        String filter = ctx.request().params().get(FILTER);
        eventBus.send(redisquesAddress, buildGetQueuesCountOperation(filter), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonObject result = new JsonObject();
                result.put("count", reply.result().body().getLong(VALUE));
                jsonResponse(ctx.response(), result);
            } else {
                String error = "Error gathering count of active queues. Cause: " + reply.result().body().getString(MESSAGE);
                String errorType = reply.result().body().getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                    respondWith(StatusCode.BAD_REQUEST, error, ctx.request());
                } else {
                    respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error gathering count of active queues", ctx.request());
                }
            }
        });
    }

    private void listQueues(RoutingContext ctx) {
        String filter = ctx.request().params().get(FILTER);
        eventBus.send(redisquesAddress, buildGetQueuesOperation(filter), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                jsonResponse(ctx.response(), reply.result().body().getJsonObject(VALUE));
            } else {
                String error = "Unable to list active queues. Cause: " + reply.result().body().getString(MESSAGE);
                String errorType = reply.result().body().getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                    respondWith(StatusCode.BAD_REQUEST, error, ctx.request());
                } else {
                    respondWith(StatusCode.INTERNAL_SERVER_ERROR, error, ctx.request());
                }
            }
        });
    }

    private void listOrCountQueueItems(RoutingContext ctx) {
        if (evaluateUrlParameterToBeEmptyOrTrue(COUNT_PARAM, ctx.request())) {
            getQueueItemsCount(ctx);
        } else {
            listQueueItems(ctx);
        }
    }

    private void listQueueItems(RoutingContext ctx) {
        final String queue = lastPart(ctx.request().path());
        String limitParam = null;
        if (ctx.request() != null && ctx.request().params().contains(LIMIT)) {
            limitParam = ctx.request().params().get(LIMIT);
        }
        eventBus.send(redisquesAddress, buildGetQueueItemsOperation(queue, limitParam), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.failed()) {
                log.error("Received failed message for listQueueItemsOperation. _757074e6af79b816f2942_", reply.cause());
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, ctx.request());
                return;
            }
            final JsonObject replyBody = reply.result().body();
            if (OK.equals(replyBody.getString(STATUS))) {
                List<Object> list = replyBody.getJsonArray(VALUE).getList();
                JsonArray items = new JsonArray();
                for (Object item : list.toArray()) {
                    items.add((String) item);
                }
                JsonObject result = new JsonObject().put(queue, items);
                jsonResponse(ctx.response(), result);
            } else {
                ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                ctx.response().end(replyBody.getString(MESSAGE));
                log.warn("Error in routerMatcher.getWithRegEx. Command = '" + (replyBody.getString("command") == null ? "<null>" : replyBody.getString("command")) + "'.");
            }
        });
    }

    private void addQueueItem(RoutingContext ctx) {
        final String queue = part(ctx.request().path(), 1);
        ctx.request().bodyHandler(buffer -> {
            try {
                String strBuffer = encodePayload(buffer.toString());
                eventBus.send(redisquesAddress, buildAddQueueItemOperation(queue, strBuffer),
                        (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                            if (reply.failed()) {
                                log.warn("Received failed message for addQueueItemOperation. Lets run into NullPointerException now. _1a06e3030ddf3775415_", reply.cause());
                                // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
                                // Nevertheless. Lets run into NullPointerException by calling method below.
                            }
                            checkReply(reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
                        });
            } catch (Exception ex) {
                respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
            }
        });
    }

    private void getSingleQueueItem(RoutingContext ctx) {
        final String requestPath = ctx.request().path();
        final String queue = lastPart(requestPath.substring(0, requestPath.length() - 2));
        final int index = Integer.parseInt(lastPart(requestPath));
        eventBus.send(redisquesAddress, buildGetQueueItemOperation(queue, index), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            if (reply.failed()) {
                log.warn("Received failed message for getSingleQueueItemOperation. Lets run into NullPointerException now. _4cb8173344dfea5ae_", reply.cause());
                // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
            }
            final JsonObject replyBody = reply.result().body();
            final HttpServerResponse response = ctx.response();
            if (OK.equals(replyBody.getString(STATUS))) {
                response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
                response.end(decode(replyBody.getString(VALUE)));
            } else {
                response.setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                response.setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                response.end("Not Found");
            }
        });
    }

    private void replaceSingleQueueItem(RoutingContext ctx) {
        final HttpServerRequest request = ctx.request();
        final String queue = part(request.path(), 2);
        checkLocked(queue, request, voidEvent -> {
            final int index = Integer.parseInt(lastPart(request.path()));
            request.bodyHandler(buffer -> {
                try {
                    String strBuffer = encodePayload(buffer.toString());
                    eventBus.send(redisquesAddress, buildReplaceQueueItemOperation(queue, index, strBuffer),
                            (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                                if (reply.failed()) {
                                    log.warn("Received failed message for replaceSingleQueueItemOperation. Lets run into NullPointerException now. _fc928c9bda5d5ac26c562_", reply.cause());
                                    // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
                                    // Nevertheless. Lets run into NullPointerException by calling method below.
                                }
                                checkReply(reply.result(), request, StatusCode.NOT_FOUND);
                            });
                } catch (Exception ex) {
                    log.warn("Undocumented exception caught while replaceSingleQueueItem. But assume its the clients bad ;)", ex);
                    respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), request);
                }
            });
        });
    }

    private void deleteQueueItem(RoutingContext ctx) {
        final HttpServerRequest request = ctx.request();
        final String queue = part(request.path(), 2);
        final int index = Integer.parseInt(lastPart(request.path()));
        checkLocked(queue, request, event -> {
            eventBus.send(redisquesAddress, buildDeleteQueueItemOperation(queue, index),
                    (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                        if (reply.failed()) {
                            log.warn("Received failed message for deleteQueueItemOperation. Lets run into NullPointerException now. _57d9c0bcc34c64bc5f6dc8b_", reply.cause());
                            // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
                            // Nevertheless. Lets run into NullPointerException by calling method below.
                        }
                        checkReply(reply.result(), request, StatusCode.NOT_FOUND);
                    }
            );
        });
    }

    private void deleteAllQueueItems(RoutingContext ctx) {
        final HttpServerRequest request = ctx.request();
        boolean unlock = evaluateUrlParameterToBeEmptyOrTrue(UNLOCK_PARAM, request);
        final String queue = lastPart(request.path());
        eventBus.send(redisquesAddress, buildDeleteAllQueueItemsOperation(queue, unlock), reply -> {
            if (reply.failed()) {
                log.warn("Received failed message for deleteAllQueueItemsOperation. _aa9786093e87b5ae_", reply.cause());
                // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
            }
            ctx.response().end();
        });
    }

    private void bulkDeleteQueues(RoutingContext ctx) {
        final HttpServerRequest request = ctx.request();
        if (evaluateUrlParameterToBeEmptyOrTrue(BULK_DELETE_PARAM, request)) {
            request.bodyHandler(buffer -> {
                try {
                    Result<JsonArray, String> result = extractNonEmptyJsonArrayFromBody(QUEUES, buffer.toString());
                    if (result.isErr()) {
                        respondWith(StatusCode.BAD_REQUEST, result.getErr(), request);
                        return;
                    }
                    eventBus.send(redisquesAddress, buildBulkDeleteQueuesOperation(result.getOk()), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                        if (reply.failed()) {
                            log.warn("Failed to bulkDeleteQueues. Lets run into NullPointerException now. _969d89cdf7f8f4dd5c49b_", reply.cause());
                            // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward compatibility.
                            // Nevertheless. Lets run into NullPointerException in else case below now.
                        }
                        if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                            JsonObject resultObj = new JsonObject();
                            resultObj.put(DELETED, reply.result().body().getLong(VALUE));
                            jsonResponse(ctx.response(), resultObj);
                        } else {
                            final JsonObject body = reply.result().body();
                            String errorType = body.getString(ERROR_TYPE);
                            if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                                if (body.getString(MESSAGE) != null) {
                                    respondWith(StatusCode.BAD_REQUEST, body.getString(MESSAGE), request);
                                } else {
                                    respondWith(StatusCode.BAD_REQUEST, request);
                                }
                            } else {
                                respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error bulk deleting queues", request);
                            }
                        }
                    });

                } catch (Exception ex) {
                    respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), request);
                }
            });
        } else {
            respondWith(StatusCode.BAD_REQUEST, "Unsupported operation. Add '" + BULK_DELETE_PARAM + "' parameter for bulk deleting queues", request);
        }
    }

    private void respondWith(StatusCode statusCode, String responseMessage, HttpServerRequest request) {
        final HttpServerResponse response = request.response();
        log.info("Responding with status code " + statusCode + " and message: " + responseMessage);
        response.setStatusCode(statusCode.getStatusCode());
        response.setStatusMessage(statusCode.getStatusMessage());
        response.end(responseMessage);
    }

    private void respondWith(StatusCode statusCode, HttpServerRequest request) {
        respondWith(statusCode, statusCode.getStatusMessage(), request);
    }

    private String lastPart(String source) {
        String[] tokens = source.split("/");
        return tokens[tokens.length - 1];
    }

    private String part(String source, int pos) {
        String[] tokens = source.split("/");
        return tokens[tokens.length - pos];
    }

    private void jsonResponse(HttpServerResponse response, JsonObject object) {
        response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
        response.end(object.encode());
    }

    private String extractUser(HttpServerRequest request) {
        String user = request.headers().get(userHeader);
        if (user == null) {
            user = "Unknown";
        }
        return user;
    }

    private void checkLocked(String queue, final HttpServerRequest request, final Handler<Void> handler) {
        request.pause();
        eventBus.send(redisquesAddress, buildGetLockOperation(queue), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
            request.resume();
            if (NO_SUCH_LOCK.equals(reply.result().body().getString(STATUS))) {
                final HttpServerResponse response = request.response();
                response.setStatusCode(StatusCode.CONFLICT.getStatusCode());
                response.setStatusMessage("Queue must be locked to perform this operation");
                response.end("Queue must be locked to perform this operation");
            } else {
                handler.handle(null);
            }
        });
    }

    private void checkReply(Message<JsonObject> reply, HttpServerRequest request, StatusCode statusCode) {
        final HttpServerResponse response = request.response();
        if (OK.equals(reply.body().getString(STATUS))) {
            response.end();
        } else {
            response.setStatusCode(statusCode.getStatusCode());
            response.setStatusMessage(statusCode.getStatusMessage());
            response.end(statusCode.getStatusMessage());
        }
    }

    private int extractLimit(RoutingContext ctx) {
        String limitParam = ctx.request().params().get(LIMIT);
        try {
            return Integer.parseInt(limitParam);
        } catch (NumberFormatException ex) {
            if (limitParam != null) {
                log.warn("Non-numeric limit parameter value used: " + limitParam);
            }
            return Integer.MAX_VALUE;
        }
    }

    private void collectQueueLengths(final List<String> queueNames, final int limit, final boolean showEmptyQueues, final QueueLengthCollectingCallback callback) {
        final SortedMap<String, Long> resultMap = new TreeMap<>();
        final List<Map.Entry<String, Long>> mapEntryList = new ArrayList<>();
        final AtomicInteger subCommandCount = new AtomicInteger(queueNames.size());
        if (!queueNames.isEmpty()) {
            for (final String name : queueNames) {
                eventBus.send(redisquesAddress, buildGetQueueItemsCountOperation(name), (Handler<AsyncResult<Message<JsonObject>>>) reply -> {
                    subCommandCount.decrementAndGet();
                    if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                        final long count = reply.result().body().getLong(VALUE);
                        if (showEmptyQueues || count > 0) {
                            resultMap.put(name, count);
                        }
                    } else {
                        log.error("Error gathering size of queue " + name);
                    }

                    if (subCommandCount.get() == 0) {
                        mapEntryList.addAll(resultMap.entrySet());
                        sortResultMap(mapEntryList);
                        int toIndex = limit > queueNames.size() ? queueNames.size() : limit;
                        toIndex = Math.min(mapEntryList.size(), toIndex);
                        callback.onDone(mapEntryList.subList(0, toIndex));
                    }
                });
            }
        } else {
            callback.onDone(mapEntryList);
        }
    }

    private interface QueueLengthCollectingCallback {
        void onDone(List<Map.Entry<String, Long>> mapEntries);
    }

    private void sortResultMap(List<Map.Entry<String, Long>> input) {
        input.sort((left, right) -> right.getValue().compareTo(left.getValue()));
    }
}
