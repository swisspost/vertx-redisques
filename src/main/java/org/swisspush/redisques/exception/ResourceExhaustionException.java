package org.swisspush.redisques.exception;

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
    }

}
