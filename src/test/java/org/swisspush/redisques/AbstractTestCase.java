package org.swisspush.redisques;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.swisspush.redisques.util.RedisquesConfiguration;
import redis.clients.jedis.Jedis;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public abstract class AbstractTestCase {

    public static final int NUM_QUEUES = 10;
    public static final String OK = "ok";
    public static final String ERROR = "error";
    public static final String VALUE = "value";
    public static final String INFO = "info";
    public static final String STATUS = "status";
    public static final String INDEX = "index";
    public static final String BUFFER = "buffer";
    public static final String MESSAGE = "message";
    public static final String PAYLOAD = "payload";
    public static final String REQUESTEDBY = "requestedBy";
    public static final String TIMESTAMP = "timestamp";
    public static final String QUEUENAME = "queuename";
    public static final String LIMIT = "limit";
    public static final String QUEUES_PREFIX = "redisques:queues:";
    public static final String REDISQUES_LOCKS = "redisques:locks";

    @Rule
    public Timeout rule = Timeout.seconds(5);

    static Vertx vertx;
    static Logger log = LoggerFactory.getLogger(AbstractTestCase.class);
    protected static Jedis jedis;

    public enum Operation{
        addItem, deleteItem, deleteLock, getAllLocks, getItem, getLock, enqueue, getListRange, deleteAllQueueItems, putLock, replaceItem
    }

    protected void flushAll(){
        if(jedis != null){
            jedis.flushAll();
        }
    }

    protected void assertKeyCount(TestContext context, int keyCount){
        assertKeyCount(context, "", keyCount);
    }

    protected void assertKeyCount(TestContext context, String prefix, int keyCount){
        context.assertEquals(keyCount, jedis.keys(prefix+"*").size());
    }

    @BeforeClass
    public static void config(TestContext context) {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.start();
        }
        setUp(context);
    }

    @AfterClass
    public static void stopRedis(TestContext context) {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.stop();
        }
        jedis.close();
    }

    private static void setUp(TestContext context) {
        vertx = Vertx.vertx();
        initProcessor(vertx.eventBus());

        JsonObject config = RedisquesConfiguration.with()
                .processorAddress("processor-address")
                .redisEncoding("ISO-8859-1")
                .build()
                .asJsonObject();

        RedisQues redisQues = new RedisQues();
        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
        }));
    }

    private static void initProcessor(EventBus eventBus){

        final Map<String, Integer> counters = new HashMap<>();
        final Map<String, MessageDigest> signatures = new HashMap<>();

        eventBus.consumer("processor-address", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                final String queue = message.body().getString("queue");
                final String payload = message.body().getString("payload");

                if(!counters.containsKey(queue)) {
                    counters.put(queue, 0);
                }

                counters.put(queue, counters.get(queue)+1);

                if ("STOP".equals(payload)) {
                    message.reply(new JsonObject() {
                        {
                            put("status", "ok");
                        }
                    });
                    eventBus.send("digest-" + queue, DatatypeConverter.printBase64Binary(signatures.get(queue).digest()));
                } else {
                    MessageDigest signature = signatures.get(queue);
                    if (signature == null) {
                        try {
                            signature = MessageDigest.getInstance("MD5");
                            signatures.put(queue, signature);
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException();
                        }
                    }
                    signature.update(payload.getBytes());
                }

                vertx.setTimer(new Random().nextLong() % 1 + 1, event -> {
                    log.debug("Processed message " + payload);
                    message.reply(new JsonObject() {
                        {
                            put("status", "ok");
                        }
                    });
                });
            }
        });
    }

    @Before
    public void cleanDB() {
        flushAll();
    }

    public JsonObject enqueueOperation(String queueName, String message){
        JsonObject operation = buildOperation(Operation.enqueue);
        operation.put(PAYLOAD, new JsonObject().put(QUEUENAME, queueName));
        operation.put(MESSAGE, message);
        return operation;
    }

    public JsonObject putLockOperation(String queueName, String requestedBy){
        JsonObject operation = buildOperation(Operation.putLock);
        operation.put(PAYLOAD, new JsonObject().put(QUEUENAME, queueName).put(REQUESTEDBY, requestedBy));
        return operation;
    }

    public JsonObject buildOperation(Operation operation){
        JsonObject op = new JsonObject();
        op.put("operation", operation.name());
        return op;
    }

    public JsonObject buildOperation(Operation operation, JsonObject payload){
        JsonObject op = buildOperation(operation);
        op.put(PAYLOAD, payload);
        return op;
    }

    int numMessages = 5;
    AtomicInteger finished = new AtomicInteger();

    /**
     * Sender Class
     */
    class Sender {
        final String queue;
        int messageCount;
        MessageDigest signature;
        TestContext context;
        Async async;

        Sender(TestContext context, Async async, final String queue) {
            this.context = context;
            this.async = async;
            this.queue = queue;
            try {
                signature = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            vertx.eventBus().consumer("digest-" + queue, new Handler<Message<String>>() {
                @Override
                public void handle(Message<String> event) {
                    log.info("Received signature for " + queue + ": " + event.body());
                    context.assertEquals(event.body(), DatatypeConverter.printBase64Binary(signature.digest()), "Signatures differ");
                    if (finished.incrementAndGet() == NUM_QUEUES) {
                        async.complete();
                    }
                }
            });
        }

        void send(final String m) {
            if (messageCount < numMessages) {
                final String message;
                if(m==null) {
                    message = Double.toString(Math.random());
                } else {
                    message = m;
                }
                signature.update(message.getBytes());
                vertx.eventBus().send("redisques", enqueueOperation(queue, message), new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> event) {
                        if(event.result().body().getString(STATUS).equals(OK)) {
                            send(null);
                        } else {
                            log.error("ERROR sending "+message+" to "+queue);
                            send(message);
                        }
                    }
                });
                messageCount++;
            } else {
                vertx.eventBus().send("redisques", enqueueOperation(queue, "STOP"), new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                        context.assertEquals(OK, reply.result().body().getString(STATUS));
                    }
                });
            }
        }
    }
}