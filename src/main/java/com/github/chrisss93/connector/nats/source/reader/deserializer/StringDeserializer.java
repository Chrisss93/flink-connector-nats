package com.github.chrisss93.connector.nats.source.reader.deserializer;

import io.nats.client.Message;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class StringDeserializer implements NatsMessageDeserializationSchema<String> {
    public String deserialize(Message message) {
        return new String(message.getData());
    }

    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}