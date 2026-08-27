package org.swisspush.redisques.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Creates and tracks Vert.x event-bus consumers so their lifecycle can be managed from one place.
 */
public class MessageConsumerManager {

    private final Vertx vertx;
    private final List<MessageConsumer<?>> consumers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MessageConsumerManager(Vertx vertx) {
        this.vertx = Objects.requireNonNull(vertx);
    }

    /**
     * Creates a consumer and adds it to the managed consumer set.
     */
    public synchronized <T> MessageConsumer<T> consumer(String address) {
        assertOpen();
        MessageConsumer<T> consumer = vertx.eventBus().consumer(address);
        consumers.add(consumer);
        return consumer;
    }

    /**
     * Creates a consumer with a handler and adds it to the managed consumer set.
     */
    public synchronized <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
        assertOpen();
        MessageConsumer<T> consumer = vertx.eventBus().consumer(address, handler);
        consumers.add(consumer);
        return consumer;
    }

    /**
     * Adds an already-created consumer to the managed consumer set.
     */
    public synchronized <T> MessageConsumer<T> manage(MessageConsumer<T> consumer) {
        assertOpen();
        consumers.add(Objects.requireNonNull(consumer));
        return consumer;
    }

    /**
     * Unregisters one managed consumer and removes it from the managed consumer set.
     */
    public Future<Void> unregister(MessageConsumer<?> consumer) {
        Objects.requireNonNull(consumer);
        Future<Void> unregisterFuture = consumer.isRegistered() ? consumer.unregister() : Future.succeededFuture();
        return unregisterFuture.onSuccess(event -> remove(consumer));
    }

    /**
     * Unregisters all currently registered managed consumers and clears the managed consumer set.
     */
    public Future<Void> unregisterAll() {
        List<MessageConsumer<?>> managedConsumers = closeAndGetManagedConsumers();
        List<MessageConsumer<?>> registeredConsumers = managedConsumers.stream()
                .filter(MessageConsumer::isRegistered)
                .collect(Collectors.toList());
        if (registeredConsumers.isEmpty()) {
            removeAll(managedConsumers);
            return Future.succeededFuture();
        }
        List<Future<Void>> unregisterFutures = registeredConsumers.stream()
                .map(MessageConsumer::unregister)
                .collect(Collectors.toList());
        Future<Void> unregisterFuture = Future.join(unregisterFutures).mapEmpty();
        unregisterFuture.onSuccess(event -> removeAll(managedConsumers));
        return unregisterFuture;
    }

    public void unregisterAll(Handler<AsyncResult<Void>> handler) {
        unregisterAll().onComplete(handler);
    }

    public synchronized int size() {
        return consumers.size();
    }

    public boolean isClosed() {
        return closed.get();
    }

    private synchronized List<MessageConsumer<?>> closeAndGetManagedConsumers() {
        closed.set(true);
        return new CopyOnWriteArrayList<>(consumers);
    }

    private void assertOpen() {
        if (closed.get()) {
            throw new IllegalStateException("MessageConsumerManager is already closed");
        }
    }

    private synchronized void remove(MessageConsumer<?> consumer) {
        consumers.remove(consumer);
    }

    private synchronized void removeAll(List<MessageConsumer<?>> consumersToRemove) {
        consumers.removeAll(consumersToRemove);
    }
}
