package org.swisspush.redisques.exception;

import java.util.Arrays;

/**
 * <p>Thrown when something cannot be done right now because some
 * resource is exhausted.</p>
 *
 * <p>For example not enough memory, connection pools or waiting queues
 * are full, etc.</p>
 */
public class ResourceExhaustionException extends Exception {

    public ResourceExhaustionException(String message, Throwable cause) {
        super(message, cause);
        // only keep 4 lines of stack trace for this exception
        this.setStackTrace(Arrays.copyOf(this.getStackTrace(), Math.min(4, this.getStackTrace().length)));
    }
}
