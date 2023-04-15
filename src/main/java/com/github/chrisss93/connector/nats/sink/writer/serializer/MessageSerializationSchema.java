package com.github.chrisss93.connector.nats.sink.writer.serializer;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.Serializable;

public interface MessageSerializationSchema<T> extends Serializable {
    String timestampHeaderKey = "flink.event.timestamp";

    byte[] serialize(T element);

    String getSubject(T element);

    default void open(SerializationSchema.InitializationContext context) {}

    default Message makeMessage(T element, Long timestamp) {
        return new NatsMessage(
            getSubject(element),
            "",
            new Headers().add(timestampHeaderKey, String.valueOf(timestamp)),
            serialize(element)
        );
    }
}