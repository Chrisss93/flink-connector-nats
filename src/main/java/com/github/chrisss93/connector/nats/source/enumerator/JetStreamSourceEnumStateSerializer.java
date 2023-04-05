package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitSerializer;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
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

public class JetStreamSourceEnumStateSerializer implements SimpleVersionedSerializer<JetStreamSourceEnumState> {
    public static JetStreamSourceEnumStateSerializer INSTANCE = new JetStreamSourceEnumStateSerializer();
    @Override
    public int getVersion() {
        return JetStreamConsumerSplitSerializer.INSTANCE.getVersion();
    }

    @Override
    public byte[] serialize(JetStreamSourceEnumState obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(obj.getAssignedSplits().size());
            for (JetStreamConsumerSplit split : obj.getAssignedSplits()) {
                JetStreamConsumerSplitSerializer.write(split, out);
            }

            out.writeInt(obj.getPendingSplitAssignments().size());
            for (Map.Entry<Integer, JetStreamConsumerSplit> entry : obj.getPendingSplitAssignments().entrySet()) {
                out.writeInt(entry.getKey());
                JetStreamConsumerSplitSerializer.write(entry.getValue(), out);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public JetStreamSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {

            int assignedSize = in.readInt();
            Set<JetStreamConsumerSplit> assigned = new HashSet<>(assignedSize);
            for (int i = 0; i < assignedSize; i++) {
                assigned.add(JetStreamConsumerSplitSerializer.read(in));
            }

            int pendingSize = in.readInt();
            Map<Integer, JetStreamConsumerSplit> pending = new HashMap<>(pendingSize);
            for (int i = 0; i < pendingSize; i++) {
                int k = in.readInt();
                JetStreamConsumerSplit v = JetStreamConsumerSplitSerializer.read(in);
                pending.put(k, v);
            }
            return new JetStreamSourceEnumState(assigned, pending);
        }
    }
}
