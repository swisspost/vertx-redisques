package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.function.Supplier;

/**
 * a provider that guarantees only one instance per Vert.x instance in vertx style, when multiple Verticles
 * @param <T> type
 */
public class NodeLocalSingletonProvider<T> {
    private final static String LOCAL_SINGLETON_PROVIDER_MAP_KEY = "redisques-singletons:";
    private final Vertx vertx;
    private final String key;
    private final Supplier<Future<T>> factory;

    public NodeLocalSingletonProvider(Vertx vertx, String key, Supplier<Future<T>> factory) {
        this.vertx = vertx;
        this.key = key;
        this.factory = factory;
    }

    public Future<T> get() {
        T existing = NodeLocalObjectRegistry.get(key);
        if (existing != null) {
            return Future.succeededFuture(existing);
        }

        return vertx.sharedData().getLocalLock(LOCAL_SINGLETON_PROVIDER_MAP_KEY + key).compose(lock -> {

            T again = NodeLocalObjectRegistry.get(key);
            if (again != null) {
                lock.release();
                return Future.succeededFuture(again);
            }

            return factory.get()
                    .onSuccess(obj -> NodeLocalObjectRegistry.put(key, obj))
                    .onComplete(v -> lock.release());
        });
    }
}