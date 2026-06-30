package org.swisspush.redisques.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class QueueSizeInfoMapCodec implements MessageCodec<QueueSizeInfoMap, QueueSizeInfoMap> {

    @Override
    public void encodeToWire(Buffer buffer, QueueSizeInfoMap body) {
        JsonObject root = new JsonObject();

        body.getValue().forEach((outerKey, innerMap) -> {
            JsonObject innerJson = new JsonObject();

            innerMap.forEach((queueName, info) -> {
                innerJson.put(queueName, JsonObject.mapFrom(info));
            });

            root.put(outerKey, innerJson);
        });

        String json = root.encode();
        buffer.appendInt(json.getBytes().length);
        buffer.appendString(json);
    }

    @Override
    public QueueSizeInfoMap decodeFromWire(int pos, Buffer buffer) {
        final QueueSizeInfoMap  queueSizeInfoMap = new QueueSizeInfoMap();
        int length = buffer.getInt(pos);
        pos += 4;

        String json = buffer.getString(pos, pos + length);
        JsonObject root = new JsonObject(json);

        root.forEach(outerEntry -> {
            String outerKey = outerEntry.getKey();
            JsonObject innerJson = (JsonObject) outerEntry.getValue();

            Map<String, QueueSizeInfoEntry> innerMap = new HashMap<>();

            innerJson.forEach(innerEntry -> {
                String queueName = innerEntry.getKey();
                JsonObject infoJson = (JsonObject) innerEntry.getValue();

                QueueSizeInfoEntry info = infoJson.mapTo(QueueSizeInfoEntry.class);
                innerMap.put(queueName, info);
            });

            queueSizeInfoMap.getValue().put(outerKey, innerMap);
        });

        return queueSizeInfoMap;
    }

    @Override
    public QueueSizeInfoMap transform(QueueSizeInfoMap body) {
        return body;
    }

    @Override
    public String name() {
        return "QueueSizeInfoMapCodec";
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}