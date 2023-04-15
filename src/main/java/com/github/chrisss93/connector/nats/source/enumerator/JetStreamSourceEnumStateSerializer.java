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

            out.writeInt(obj.getAssignedSplits().size());
            for (JetStreamConsumerSplit split : obj.getAssignedSplits()) {
                JetStreamConsumerSplit.write(split, out);
            }

            out.writeInt(obj.getPendingAssignments().size());
            for (Map.Entry<Integer, Set<JetStreamConsumerSplit>> entry : obj.getPendingAssignments().entrySet()) {
                out.writeInt(entry.getKey());
                out.writeInt(entry.getValue().size());
                for (JetStreamConsumerSplit split : entry.getValue()) {
                    JetStreamConsumerSplit.write(split, out);
                }
            }
            out.flush();
            return bytes.toByteArray();
        }
    }

    @Override
    public JetStreamSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
             ObjectInputStream in = new ObjectInputStream(bytes)) {

            int assignedSize = in.readInt();
            Set<JetStreamConsumerSplit> assigned = new HashSet<>(assignedSize);
            for (int i = 0; i < assignedSize; i++) {
                try {
                    assigned.add(JetStreamConsumerSplit.read(in));
                } catch (ClassNotFoundException e) {
                    throw new FlinkRuntimeException(e);
                }
            }

            int pendingSize = in.readInt();
            Map<Integer, Set<JetStreamConsumerSplit>> pending = new HashMap<>(pendingSize);
            for (int i = 0; i < pendingSize; i++) {
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
                pending.put(k, splits);
            }
            return new JetStreamSourceEnumState(assigned, pending);
        }
    }
}
