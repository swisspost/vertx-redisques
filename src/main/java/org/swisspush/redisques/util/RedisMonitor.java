package org.swisspush.redisques.util;

import com.google.common.base.Splitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisMonitor {
    private final Vertx vertx;
    private final RedisProvider redisProvider;
    private final int period;
    private long timer;
    private final Logger log = LoggerFactory.getLogger(RedisMonitor.class);

    private static final String DELIMITER = ":";

    private final MetricsPublisher publisher;

    /**
     * @param vertx         vertx
     * @param redisProvider RedisProvider
     * @param name          name
     * @param period        in seconds.
     */
    public RedisMonitor(Vertx vertx, RedisProvider redisProvider, String monitoringAddress, String name, int period) {
        this(vertx, redisProvider, name, period,
                new EventBusMetricsPublisher(vertx, monitoringAddress, "redis." + name + ".")
        );
    }

    public RedisMonitor(Vertx vertx, RedisProvider redisProvider, String name, int period, MetricsPublisher publisher) {
        this.vertx = vertx;
        this.redisProvider = redisProvider;
        this.period = period * 1000;
        this.publisher = publisher;
    }

    public void start() {
        timer = vertx.setPeriodic(period, timer -> redisProvider.redis().onSuccess(redisAPI -> {
            redisAPI.info(new ArrayList<>()).onComplete(event -> {
                if (event.succeeded()) {
                    collectMetrics(event.result().toBuffer());
                } else {
                    log.warn("Cannot collect INFO from redis");
                }
            });
        }).onFailure(throwable -> log.warn("Cannot collect INFO from redis", throwable)));
    }

    public void stop() {
        if (timer != 0) {
            vertx.cancelTimer(timer);
        }
    }

    private void collectMetrics(Buffer buffer) {
        Map<String, String> map = new HashMap<>();

        Splitter.on(System.lineSeparator()).omitEmptyStrings()
                .trimResults().splitToList(buffer.toString()).stream()
                .filter(input -> input != null && input.contains(DELIMITER)
                        && !input.contains("executable")
                        && !input.contains("config_file")).forEach(entry -> {
                    List<String> keyValue = Splitter.on(DELIMITER).omitEmptyStrings().trimResults().splitToList(entry);
                    if (keyValue.size() == 2) {
                        map.put(keyValue.get(0), keyValue.get(1));
                    }
                });

        log.debug("got redis metrics {}", map);

        map.forEach((key, valueStr) -> {
            long value;
            try {
                if (key.startsWith("db")) {
                    String[] pairs = valueStr.split(",");
                    for (String pair : pairs) {
                        String[] tokens = pair.split("=");
                        if (tokens.length == 2) {
                            value = Long.parseLong(tokens[1]);
                            publisher.publishMetric("keyspace." + key + "." + tokens[0], value);
                        } else {
                            log.warn("Invalid keyspace property. Will be ignored");
                        }
                    }
                } else if (key.contains("_cpu_")) {
                    value = (long) (Double.parseDouble(valueStr) * 1000.0);
                    publisher.publishMetric(key, value);
                } else if (key.contains("fragmentation_ratio")) {
                    value = (long) (Double.parseDouble(valueStr));
                    publisher.publishMetric(key, value);
                } else {
                    value = Long.parseLong(valueStr);
                    publisher.publishMetric(key, value);
                }
            } catch (NumberFormatException e) {
                // ignore this field
            }
        });
    }
}
