package com.github.chrisss93.connector.nats.source.reader.deserializer;

import io.nats.client.Message;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class StringDeserializer implements NATSMessageDeserializationSchema<String> {
    public String deserialize(Message message) {
        return new String(message.getData());
    }

    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}