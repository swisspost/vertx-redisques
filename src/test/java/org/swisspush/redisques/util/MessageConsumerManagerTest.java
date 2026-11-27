package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MessageConsumerManagerTest {

    private Vertx vertx;
    private MessageConsumerManager manager;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        manager = new MessageConsumerManager(vertx);
    }

    @After
    public void tearDown(TestContext context) {
        Async async = context.async();
        manager.unregisterAll()
                .compose(event -> vertx.close())
                .onComplete(context.asyncAssertSuccess(event -> async.complete()));
    }

    @Test
    public void testConsumerCreatesAndTracksConsumer(TestContext context) {
        Async async = context.async();
        String address = address();

        MessageConsumer<String> consumer = manager.consumer(address, message -> message.reply("pong"));
        consumer.completionHandler(context.asyncAssertSuccess(event -> {
            context.assertEquals(1, manager.size());
            vertx.eventBus().<String>request(address, "ping", context.asyncAssertSuccess(reply -> {
                context.assertEquals("pong", reply.body());
                async.complete();
            }));
        }));
    }

    @Test
    public void testManageTracksExistingConsumer(TestContext context) {
        Async async = context.async();
        String address = address();
        MessageConsumer<String> consumer = vertx.eventBus().consumer(address, message -> message.reply("managed"));

        MessageConsumer<String> managedConsumer = manager.manage(consumer);
        consumer.completionHandler(context.asyncAssertSuccess(event -> {
            context.assertTrue(consumer == managedConsumer);
            context.assertEquals(1, manager.size());
            vertx.eventBus().<String>request(address, "ping", context.asyncAssertSuccess(reply -> {
                context.assertEquals("managed", reply.body());
                async.complete();
            }));
        }));
    }

    @Test
    public void testUnregisterRemovesSingleConsumer(TestContext context) {
        Async async = context.async();
        String address = address();
        MessageConsumer<String> consumer = manager.consumer(address, message -> message.reply("pong"));

        consumer.completionHandler(context.asyncAssertSuccess(event -> manager.unregister(consumer)
                .onComplete(context.asyncAssertSuccess(unregistered -> {
                    context.assertEquals(0, manager.size());
                    context.assertFalse(consumer.isRegistered());
                    assertNoConsumer(context, address, async);
                }))));
    }

    @Test
    public void testUnregisterAllRemovesAllConsumers(TestContext context) {
        Async async = context.async();
        MessageConsumer<String> first = manager.consumer(address(), message -> message.reply("first"));
        MessageConsumer<String> second = manager.consumer(address(), message -> message.reply("second"));

        Future.all(registrationFuture(first), registrationFuture(second)).onComplete(context.asyncAssertSuccess(event -> {
            context.assertEquals(2, manager.size());
            context.assertFalse(manager.isClosed());
            manager.unregisterAll().onComplete(context.asyncAssertSuccess(unregistered -> {
                context.assertEquals(0, manager.size());
                context.assertTrue(manager.isClosed());
                context.assertFalse(first.isRegistered());
                context.assertFalse(second.isRegistered());
                async.complete();
            }));
        }));
    }

    @Test
    public void testUnregisterAllClearsAlreadyUnregisteredConsumers(TestContext context) {
        Async async = context.async();
        MessageConsumer<String> first = manager.consumer(address(), message -> message.reply("first"));
        MessageConsumer<String> second = manager.consumer(address(), message -> message.reply("second"));

        Future.all(registrationFuture(first), registrationFuture(second))
                .compose(event -> first.unregister())
                .compose(event -> manager.unregisterAll())
                .onComplete(context.asyncAssertSuccess(event -> {
                    context.assertEquals(0, manager.size());
                    context.assertTrue(manager.isClosed());
                    context.assertFalse(first.isRegistered());
                    context.assertFalse(second.isRegistered());
                    async.complete();
                }));
    }

    @Test
    public void testRejectsNewConsumersAfterUnregisterAll(TestContext context) {
        Async async = context.async();

        manager.unregisterAll().onComplete(context.asyncAssertSuccess(event -> {
            context.assertTrue(manager.isClosed());
            try {
                manager.consumer(address(), message -> message.reply("pong"));
                context.fail("Expected manager to reject new consumers after close");
            } catch (IllegalStateException e) {
                context.assertEquals("MessageConsumerManager is already closed", e.getMessage());
                async.complete();
            }
        }));
    }

    @Test
    public void testRejectsManagedConsumerAfterUnregisterAll(TestContext context) {
        Async async = context.async();

        manager.unregisterAll().onComplete(context.asyncAssertSuccess(event -> {
            MessageConsumer<String> consumer = vertx.eventBus().consumer(address(), message -> message.reply("managed"));
            try {
                manager.manage(consumer);
                context.fail("Expected manager to reject managed consumers after close");
            } catch (IllegalStateException e) {
                consumer.unregister().onComplete(context.asyncAssertSuccess(unregistered -> async.complete()));
            }
        }));
    }

    private void assertNoConsumer(TestContext context, String address, Async async) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(100);
        vertx.eventBus().request(address, "ping", deliveryOptions, context.asyncAssertFailure(cause -> {
            context.assertTrue(cause instanceof ReplyException);
            context.assertEquals(ReplyFailure.NO_HANDLERS, ((ReplyException) cause).failureType());
            async.complete();
        }));
    }

    private Future<Void> registrationFuture(MessageConsumer<?> consumer) {
        Promise<Void> promise = Promise.promise();
        consumer.completionHandler(event -> {
            if (event.succeeded()) {
                promise.complete();
            } else {
                promise.fail(event.cause());
            }
        });
        return promise.future();
    }

    private String address() {
        return getClass().getName() + "." + UUID.randomUUID();
    }
}
