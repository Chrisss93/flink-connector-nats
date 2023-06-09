package com.github.chrisss93.connector.nats.source.reader.deserializer;

import io.nats.client.Message;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.IOException;
import java.io.Serializable;

public interface NATSMessageDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    default void open(DeserializationSchema.InitializationContext context) throws Exception {
    }
    T deserialize(Message message) throws IOException;

}
