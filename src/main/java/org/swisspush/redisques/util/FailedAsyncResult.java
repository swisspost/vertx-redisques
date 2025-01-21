package org.swisspush.redisques.util;

import io.vertx.core.AsyncResult;

public class FailedAsyncResult<Response> implements AsyncResult<Response> {
    private final Throwable cause;

    public FailedAsyncResult(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public Response result() {
        return null;
    }

    @Override
    public Throwable cause() {
        return cause;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return true;
    }
}
