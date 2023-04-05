package com.github.chrisss93.connector.nats.sink.writer.serializer;

import java.nio.charset.StandardCharsets;

public interface StringSerializer extends MessageSerializationSchema<String> {
    @Override
    default byte[] serialize(String element) {
        return element.getBytes(StandardCharsets.UTF_8);
    }
}
