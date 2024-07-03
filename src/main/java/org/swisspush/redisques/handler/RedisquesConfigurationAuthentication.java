package org.swisspush.redisques.handler;

import io.netty.util.internal.StringUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import org.swisspush.redisques.util.RedisquesConfiguration;

import java.util.Objects;
import java.util.logging.Logger;

/**
 * Custom implementation of a {@link AuthenticationProvider} using credentials from {@link RedisquesConfiguration}
 *
 * @author <a href="https://github.com/mcweba">Marc-André Weber</a>
 */
public class RedisquesConfigurationAuthentication implements AuthenticationProvider {

    private final static Logger logger = Logger.getLogger(RedisquesConfigurationAuthentication.class.getName());

    private static final String INVALID_CREDENTIALS = "invalid username/password";

    private static class User {
        final String name;
        final String password;

        private User(String name, String password) {
            this.name = Objects.requireNonNull(name);
            this.password = Objects.requireNonNull(password);
        }
    }

    private final User user;

    public RedisquesConfigurationAuthentication(RedisquesConfiguration configuration) {
        Objects.requireNonNull(configuration);

        String username = configuration.getHttpRequestHandlerUsername();
        String password = configuration.getHttpRequestHandlerPassword();

        if (StringUtil.isNullOrEmpty(username) || StringUtil.isNullOrEmpty(password)) {
            logger.warning("Username and/or password is missing/empty");
            this.user = null;
        } else {
            this.user = new User(username, password);
        }
    }

    @Override
    public void authenticate(JsonObject authInfo, Handler<AsyncResult<io.vertx.ext.auth.User>> resultHandler) {
        authenticate(new UsernamePasswordCredentials(authInfo), resultHandler);
    }

    @Override
    public void authenticate(Credentials credentials, Handler<AsyncResult<io.vertx.ext.auth.User>> resultHandler) {
        try {
            UsernamePasswordCredentials authInfo = (UsernamePasswordCredentials) credentials;
            authInfo.checkValid(null);

            if(user == null) {
                resultHandler.handle(Future.failedFuture(INVALID_CREDENTIALS));
            } else {
                if (Objects.equals(user.name, authInfo.getUsername())
                        && Objects.equals(user.password, authInfo.getPassword())) {
                    resultHandler.handle(Future.succeededFuture(io.vertx.ext.auth.User.fromName(user.name)));
                } else {
                    resultHandler.handle(Future.failedFuture(INVALID_CREDENTIALS));
                }
            }
        } catch (RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }
}
