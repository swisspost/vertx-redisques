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
        // This impl exists for speed. So why even bother creating new instances
        // if we can use already existing ones. If caller really needs another
        // instance, he should use another implementation of this factory.
        if (cause instanceof Exception) return (Exception) cause;
        return new NoStacktraceException(message, cause);
    }

    @Override
    public RuntimeException newRuntimeException(String message, Throwable cause) {
        // This impl exists for speed. So why even bother creating new instances
        // if we can use already existing ones. If caller really needs another
        // instance, he should use another implementation of this factory.
        if (cause instanceof RuntimeException) return (RuntimeException) cause;
        return new NoStacktraceException(message, cause);
    }

    @Override
    public ReplyException newReplyException(ReplyFailure failureType, int failureCode, String msg) {
        return new NoStackReplyException(failureType, failureCode, msg);
    }

    @Override
    public ResourceExhaustionException newResourceExhaustionException(String msg, Throwable cause) {
        if (cause instanceof ResourceExhaustionException) return (ResourceExhaustionException) cause;
        return new ResourceExhaustionException(msg, cause) {
            @Override public Throwable fillInStackTrace() { return null; }
        };
    }

}
