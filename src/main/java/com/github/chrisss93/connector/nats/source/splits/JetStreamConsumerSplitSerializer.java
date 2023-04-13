package com.github.chrisss93.connector.nats.source.splits;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;

public class JetStreamConsumerSplitSerializer implements SimpleVersionedSerializer<JetStreamConsumerSplit> {
    public static final JetStreamConsumerSplitSerializer INSTANCE = new JetStreamConsumerSplitSerializer();
    public static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JetStreamConsumerSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(baos)) {
            JetStreamConsumerSplit.write(split, out);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public JetStreamConsumerSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
             ObjectInputStream in = new ObjectInputStream(bytes)) {
            return JetStreamConsumerSplit.read(in);
        } catch (ClassNotFoundException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
