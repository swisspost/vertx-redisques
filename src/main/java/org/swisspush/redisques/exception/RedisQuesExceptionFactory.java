package org.swisspush.redisques.exception;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;


/**
 * Applies dependency inversion for exception instantiation.
 *
 * This class did arise because we had different use cases in different
 * applications. One of them has the need to perform fine-grained error
 * reporting. Whereas in the other application this led to performance issues.
 * So now through this abstraction, both applications can choose the behavior
 * they need.
 *
 * If dependency-injection gets applied properly, an app can even provide its
 * custom implementation to fine-tune the exact behavior even further.
 */
public interface RedisQuesExceptionFactory {

    public default Exception newException(String message) { return newException(message, null); }

    public default Exception newException(Throwable cause) { return newException(null, cause); }

    public Exception newException(String message, Throwable cause);

    public default RuntimeException newRuntimeException(String message) { return newRuntimeException(message, null); }

    public default RuntimeException newRuntimeException(Throwable cause) { return newRuntimeException(null, cause); }

    public RuntimeException newRuntimeException(String message, Throwable cause);


    /**
     * See {@link ThriftyRedisQuesExceptionFactory}.
     */
    public static RedisQuesExceptionFactory newThriftyExceptionFactory() {
        return new ThriftyRedisQuesExceptionFactory();
    }

    /**
     * See {@link WastefulRedisQuesExceptionFactory}.
     */
    public static RedisQuesExceptionFactory newWastefulExceptionFactory() {
        return new WastefulRedisQuesExceptionFactory();
    }

}
