package org.swisspush.redisques.exception;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

/**
 * Trades speed for maintainability. For example invests more resources like
 * recording stack traces (which likely provocates more logs) to get easier
 * to debug error messages and better hints of what is happening. It also
 * keeps details like 'causes' and 'suppressed' exceptions. If an app needs
 * more error details it should use {@link WastefulRedisQuesExceptionFactory}. If none
 * of those fits the apps needs, it can provide its own implementation.
 */
class WastefulRedisQuesExceptionFactory implements RedisQuesExceptionFactory {

    WastefulRedisQuesExceptionFactory() {
    }

    public Exception newException(String message, Throwable cause) {
        return new Exception(message, cause);
    }

    @Override
    public RuntimeException newRuntimeException(String message, Throwable cause) {
        return new RuntimeException(message, cause);
    }

    @Override
    public ReplyException newReplyException(ReplyFailure failureType, int failureCode, String msg, Throwable cause) {
        if (msg == null && cause != null) msg = cause.getMessage();
        ReplyException ex = new ReplyException(failureType, failureCode, msg);
        if (cause != null) ex.initCause(cause);
        return ex;
    }

    @Override
    public ResourceExhaustionException newResourceExhaustionException(String msg, Throwable cause) {
        return new ResourceExhaustionException(msg, cause);
    }

}
