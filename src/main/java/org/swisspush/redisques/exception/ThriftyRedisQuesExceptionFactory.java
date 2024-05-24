package org.swisspush.redisques.exception;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

/**
 * Trades maintainability for speed. For example prefers lightweight
 * exceptions without stacktrace recording. It may even decide to drop 'cause'
 * and 'suppressed' exceptions. If an app needs more error details it should use
 * {@link WastefulRedisQuesExceptionFactory}. If none of those fits the apps needs, it
 * can provide its own implementation.
 */
class ThriftyRedisQuesExceptionFactory implements RedisQuesExceptionFactory {

    ThriftyRedisQuesExceptionFactory() {
    }

    public Exception newException(String message, Throwable cause) {
        return new NoStacktraceException(message, cause);
    }

    @Override
    public RuntimeException newRuntimeException(String message, Throwable cause) {
        return new NoStacktraceException(message, cause);
    }

}
