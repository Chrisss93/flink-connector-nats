package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitSerializer;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JetStreamSourceEnumStateSerializer implements SimpleVersionedSerializer<JetStreamSourceEnumState> {
    public static final JetStreamSourceEnumStateSerializer INSTANCE = new JetStreamSourceEnumStateSerializer();
    @Override
    public int getVersion() {
        return JetStreamConsumerSplitSerializer.INSTANCE.getVersion();
    }

    @Override
    public byte[] serialize(JetStreamSourceEnumState obj) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bytes)) {

            serializeMap(obj.getAssignedSplits(), out);
            serializeMap(obj.getPendingAssignments(), out);
            out.flush();
            return bytes.toByteArray();
        }
    }

    @Override
    public JetStreamSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
             ObjectInputStream in = new ObjectInputStream(bytes)) {

            return new JetStreamSourceEnumState(deserializeMap(in), deserializeMap(in));
        }
    }

    private void serializeMap(Map<Integer, Set<JetStreamConsumerSplit>> map, ObjectOutputStream out)
        throws IOException {

        out.writeInt(map.size());
        for (Map.Entry<Integer, Set<JetStreamConsumerSplit>> entry : map.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (JetStreamConsumerSplit split : entry.getValue()) {
                JetStreamConsumerSplit.write(split, out);
            }
        }
    }

    private Map<Integer, Set<JetStreamConsumerSplit>> deserializeMap(ObjectInputStream in) throws IOException {
        int mSize = in.readInt();
        Map<Integer, Set<JetStreamConsumerSplit>> map = new HashMap<>(mSize);
        for (int i = 0; i < mSize; i++) {
            int k = in.readInt();
            int size = in.readInt();
            HashSet<JetStreamConsumerSplit> splits = new HashSet<>(size);
            for (int j = 0; j < size; j++) {
                try {
                    splits.add(JetStreamConsumerSplit.read(in));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            map.put(k, splits);
        }
        return map;
    }
}
