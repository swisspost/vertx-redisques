package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;
import org.swisspush.redisques.performance.UpperBoundParallel;
import org.swisspush.redisques.util.HandlerUtil;
import org.swisspush.redisques.util.RedisProvider;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;
import static java.lang.System.currentTimeMillis;
import static org.swisspush.redisques.util.RedisquesAPI.ERROR;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_NAME;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_SIZE;
import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUES;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;


public class GetQueuesItemsCountHandler implements Handler<AsyncResult<Response>> {

    private final Logger log = LoggerFactory.getLogger(GetQueuesItemsCountHandler.class);

    private final Vertx vertx;
    private final Message<JsonObject> event;
    private final Optional<Pattern> filterPattern;
    private final String queuesPrefix;
    private final RedisProvider redisProvider;
    private final UpperBoundParallel upperBoundParallel;
    private final RedisQuesExceptionFactory exceptionFactory;
    private final Semaphore redisRequestQuota;

    public GetQueuesItemsCountHandler(
            Vertx vertx,
            Message<JsonObject> event,
            Optional<Pattern> filterPattern,
            String queuesPrefix,
            RedisProvider redisProvider,
            RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisRequestQuota
    ) {
        this.vertx = vertx;
        this.event = event;
        this.filterPattern = filterPattern;
        this.queuesPrefix = queuesPrefix;
        this.redisProvider = redisProvider;
        this.upperBoundParallel = new UpperBoundParallel(vertx);
        this.exceptionFactory = exceptionFactory;
        this.redisRequestQuota = redisRequestQuota;
    }

    @Override
    public void handle(AsyncResult<Response> handleQueues) {
        if (!handleQueues.succeeded()) {
            log.warn("Concealed error", exceptionFactory.newException(handleQueues.cause()));
            event.reply(new JsonObject().put(STATUS, ERROR));
            return;
        }
        var ctx = new Object(){
            RedisAPI redis;
            Iterator<String> iter;
            List<String> queues = HandlerUtil.filterByPattern(handleQueues.result(), filterPattern);
            int iNumberResult;
            int[] queueLengths;
        };
        if (ctx.queues.isEmpty()) {
            log.debug("Queue count evaluation with empty queues");
            event.reply(new JsonObject().put(STATUS, OK).put(QUEUES, new JsonArray()));
            return;
        }
        if (redisRequestQuota.availablePermits() <= 0) {
            event.reply(exceptionFactory.newReplyException(RECIPIENT_FAILURE, 429,
                    "Too many simultaneous '" + GetQueuesItemsCountHandler.class.getSimpleName() + "' requests in progress"));
            return;
        }

        redisProvider.redis().<Void>compose((RedisAPI redis_) -> {
            ctx.redis = redis_;
            ctx.queueLengths = new int[ctx.queues.size()];
            ctx.iter = ctx.queues.iterator();
            var p = Promise.<Void>promise();
            upperBoundParallel.request(redisRequestQuota, null, new UpperBoundParallel.Mentor<Void>() {
                @Override public boolean runOneMore(BiConsumer<Throwable, Void> onLLenDone, Void unused) {
                    if (ctx.iter.hasNext()) {
                        String queue = ctx.iter.next();
                        int iNum = ctx.iNumberResult++;
                        ctx.redis.send(Command.LLEN, queuesPrefix + queue).onSuccess((Response rsp) -> {
                            ctx.queueLengths[iNum] = rsp.toInteger();
                            onLLenDone.accept(null, null);
                        }).onFailure((Throwable ex) -> {
                            onLLenDone.accept(ex, null);
                        });
                    }
                    return ctx.iter.hasNext();
                }
                @Override public boolean onError(Throwable ex, Void ctx_) {
                    p.fail(exceptionFactory.newException("Unexpected queue length result", ex));
                    return false;
                }
                @Override public void onDone(Void ctx_) {
                    p.complete();
                }
            });
            return p.future();
        }).<JsonObject>compose((Void v) -> {
            /*going to waste another threads time to produce those garbage objects*/
            return vertx.<JsonObject>executeBlocking((Promise<JsonObject> workerPromise) -> {
                assert !Thread.currentThread().getName().toUpperCase().contains("EVENTLOOP");
                long beginEpchMs = currentTimeMillis();

                JsonArray result = new JsonArray();
                for (int i = 0; i < ctx.queueLengths.length; ++i) {
                    String queueName = ctx.queues.get(i);
                    result.add(new JsonObject()
                            .put(MONITOR_QUEUE_NAME, queueName)
                            .put(MONITOR_QUEUE_SIZE, ctx.queueLengths[i]));
                }
                var obj = new JsonObject().put(STATUS, OK).put(QUEUES, result);
                long jsonCreateDurationMs = currentTimeMillis() - beginEpchMs;
                if (jsonCreateDurationMs > 10) {
                    log.info("Creating JSON with {} entries did block this tread for {}ms",
                            ctx.queueLengths.length, jsonCreateDurationMs);
                }else{
                    log.debug("Creating JSON with {} entries did block this tread for {}ms",
                            ctx.queueLengths.length, jsonCreateDurationMs);
                }
                workerPromise.complete(obj);
            }, false);
        }).onSuccess((JsonObject json) -> {
            log.trace("call event.reply(json)");
            event.reply(json);
        }).onFailure((Throwable origEx) -> {
            // For whatever reason 'event' cannot transport a regular exception. So
            // we have to squeeze it into a ReplyException.
            int failureCode = 500;
            for (Throwable thr = origEx; thr != null; thr = thr.getCause()) {
                if (thr instanceof io.vertx.core.impl.NoStackTraceThrowable && "Redis waiting queue is full".equals(thr.getMessage())) {
                    failureCode = 429;
                    break;
                }
            }
            ReplyException replyEx = exceptionFactory.newReplyException(RECIPIENT_FAILURE, failureCode, ERROR);
            replyEx.initCause(origEx);
            event.reply(replyEx);
        });
    }

}
