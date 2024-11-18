package org.swisspush.redisques;

import io.vertx.core.AsyncResult;

class FailedAsyncResult<Response> implements AsyncResult<Response> {

    private final Throwable cause;

    FailedAsyncResult(Throwable cause) {
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