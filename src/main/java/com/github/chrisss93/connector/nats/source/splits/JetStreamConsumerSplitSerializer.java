package com.github.chrisss93.connector.nats.source.splits;


import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.nats.client.support.ApiConstants.CONFIG;

// TODO : Just borrow the deserializer you already wrote in NATSConsumerConfig...
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
             DataOutputStream out = new DataOutputStream(baos)) {
            write(split, out);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public JetStreamConsumerSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            return read(in);
        }
    }

    public static void write(JetStreamConsumerSplit split, DataOutputStream out) throws IOException {
        out.writeUTF(split.getStream());
        out.writeUTF(split.getConfig().toJson());
        out.writeInt(split.getPendingAcks().size());
        for (String ack : split.getPendingAcks()) {
            out.writeUTF(ack);
        }
    }

    public static JetStreamConsumerSplit read(DataInputStream in) throws IOException {
        String stream = in.readUTF();
        String configJson = in.readUTF();
        int ackCount = in.readInt();
        Set<String> acks = new HashSet<>(ackCount);
        for (int i = 0; i < ackCount; i++) {
            acks.add(in.readUTF());
        }
        Map<String, JsonValue> m = new HashMap<>();
        m.put(CONFIG, JsonParser.parse(configJson));
        ConsumerInfo info = new ConsumerInfo(new JsonValue(m));
        return new JetStreamConsumerSplit(stream, info.getConsumerConfiguration(), acks);
    }
}
