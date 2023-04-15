package com.github.chrisss93.connector.nats.sink;

import com.github.chrisss93.connector.nats.sink.writer.serializer.MessageSerializationSchema;
import com.github.chrisss93.connector.nats.sink.writer.JetStreamWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.Properties;

public class JetStreamSink<T> implements Sink<T> {

    private final Properties connectProps;
    private final MessageSerializationSchema<T> serializationSchema;

    public JetStreamSink(Properties connectProps, MessageSerializationSchema<T> serializationSchema) {
        this.connectProps = connectProps;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) {
        return new JetStreamWriter<>(connectProps, serializationSchema, context);
    }
}