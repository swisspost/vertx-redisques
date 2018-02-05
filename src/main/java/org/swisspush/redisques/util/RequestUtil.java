package org.swisspush.redisques.util;

import io.vertx.core.http.HttpServerRequest;

/**
 * Util class to work with {@link HttpServerRequest}s
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RequestUtil {

    private static final String EMPTY = "";
    private static final String TRUE = "true";

    /**
     * Evaluates whether the provided request contains the provided url parameter and the value is either an empty
     * string or <code>true</code> (case ignored)
     *
     * @param parameter the url parameter to evaluate
     * @param request the http server request
     * @return returns true when request contains url parameter with value equal to <code>true</code> or empty string.
     */
    public static boolean evaluateUrlParameterToBeEmptyOrTrue(String parameter, HttpServerRequest request){
        if(!request.params().contains(parameter)){
            return false;
        }
        String value = request.params().get(parameter);
        return EMPTY.equalsIgnoreCase(value) || TRUE.equalsIgnoreCase(value);
    }
}
