package org.swisspush.redisques.util;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static org.swisspush.redisques.util.RedisquesAPI.PAYLOAD;

/**
 * Util class to work with {@link HttpServerRequest}s
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class HttpServerRequestUtil {

    private static final Logger log = LoggerFactory.getLogger(HttpServerRequestUtil.class);
    private static final String EMPTY = "";
    private static final String TRUE = "true";
    private static final String HEADERS = "headers";
    private static final String PAYLOAD_OBJECT = "payloadObject";
    private static final String PAYLOAD_STRING = "payloadString";
    private static final String CONTENT_LENGTH = "Content-Length";
    private static final String CONTENT_TYPE = "content-type";
    private static final String APPLICATION_JSON = "application/json";
    private static final Base64 apacheDecoder = new Base64();

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

    /**
     * Extracts a {@link JsonArray} from the provided request body. The {@link JsonArray} is not allowed to
     * be empty.
     *
     * @param property the array property to extract
     * @param requestBody the request body to extract the array from
     * @return returns a {@link Result} having the non-empty {@link JsonArray} or an error message
     */
    public static Result<JsonArray, String> extractNonEmptyJsonArrayFromBody(String property, String requestBody){
        try{
            JsonObject jsonObject = new JsonObject(requestBody);
            JsonArray jsonArray = jsonObject.getJsonArray(property);
            if(jsonArray == null) {
                return Result.err("no array called '"+property+"' found");
            }
            if(jsonArray.isEmpty()) {
                return Result.err("array '"+property+"' is not allowed to be empty");
            }
            return Result.ok(jsonArray);
        } catch (Exception ex){
            final String msg = "failed to parse request payload";
            log.error(msg, ex);
            return Result.err(msg);
        }
    }

    /**
     * Encode the payload from a payloadString or payloadObject.
     *
     * @param decoded decoded
     * @return String
     * @throws Exception
     *      In case {@code decoded} is not valid json.
     */
    public static String encodePayload(String decoded) throws Exception {
        // This may throws io.vertx.core.json.DecodeException.
        JsonObject object = new JsonObject(decoded);

        String payloadString;
        JsonObject payloadObject = object.getJsonObject(PAYLOAD_OBJECT);
        if (payloadObject != null) {
            payloadString = payloadObject.encode();
        } else {
            payloadString = object.getString(PAYLOAD_STRING);
        }

        if (payloadString != null) {
            object.put(PAYLOAD, payloadString.getBytes(StandardCharsets.UTF_8));
            object.remove(PAYLOAD_STRING);
            object.remove(PAYLOAD_OBJECT);
        }

        // update the content-length
        int length = 0;
        if (object.containsKey(PAYLOAD)) {
            length = object.getBinary(PAYLOAD).length;
        }
        JsonArray newHeaders = new JsonArray();
        for (Object headerObj : object.getJsonArray(HEADERS)) {
            JsonArray header = (JsonArray) headerObj;
            String key = header.getString(0);
            if (CONTENT_LENGTH.equalsIgnoreCase(key)) {
                JsonArray contentLengthHeader = new JsonArray();
                contentLengthHeader.add(CONTENT_LENGTH);
                contentLengthHeader.add(Integer.toString(length));
                newHeaders.add(contentLengthHeader);
            } else {
                newHeaders.add(header);
            }
        }
        object.put(HEADERS, newHeaders);

        return object.toString();
    }

    /**
     * Decode the payload if the content-type is text or json.
     *
     * @param encoded encoded
     * @return String
     */
    public static String decode(String encoded) {
        JsonObject object = new JsonObject(encoded);
        JsonArray headers = object.getJsonArray(HEADERS);
        for (Object headerObj : headers) {
            JsonArray header = (JsonArray) headerObj;
            String key = header.getString(0);
            String value = header.getString(1);
            if (key.equalsIgnoreCase(CONTENT_TYPE) && (value.contains("text/") || value.contains(APPLICATION_JSON))) {
                try {
                    object.put(PAYLOAD_OBJECT, new JsonObject(new String(apacheDecoder.decode(object.getString(PAYLOAD)), StandardCharsets.UTF_8)));
                } catch (DecodeException e) {
                    object.put(PAYLOAD_STRING, new String(apacheDecoder.decode(object.getString(PAYLOAD)), StandardCharsets.UTF_8));
                }
                object.remove(PAYLOAD);
                break;
            }
        }
        return object.toString();
    }

}
