package org.swisspush.redisques.exception;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

/**
 * Trades speed for maintainability. For example invests more resources like
 * recording stack traces (which likely provocates more logs) to get easier
 * to debug error messages and better hints of what is happening. It also
 * keeps details like 'causes' and 'suppressed' exceptions. If an app needs
 * more error details it should use {@link WastefulExceptionFactory}. If none
 * of those fits the apps needs, it can provide its own implementation.
 */
class WastefulExceptionFactory implements ExceptionFactory {

    @Override
    public ReplyException newReplyException(ReplyFailure failureType, int failureCode, String message) {
        return new ReplyException(failureType, failureCode, message);
    }

}
