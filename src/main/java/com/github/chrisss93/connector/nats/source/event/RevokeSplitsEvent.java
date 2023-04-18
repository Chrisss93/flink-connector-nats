package com.github.chrisss93.connector.nats.source.event;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import org.apache.flink.api.connector.source.SourceEvent;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

public class RevokeSplitsEvent implements SourceEvent {
    private Set<JetStreamConsumerSplit> splits;

    public RevokeSplitsEvent(Set<JetStreamConsumerSplit> splits) {
        this.splits = splits;
    }

    public Set<JetStreamConsumerSplit> getSplits() {
        return splits;
    }

    private void writeObject(ObjectOutputStream output) throws IOException {
        output.writeInt(splits.size());
        for (JetStreamConsumerSplit split : splits) {
            JetStreamConsumerSplit.write(split, output);
        }
        output.flush();
    }

    private void readObject(ObjectInputStream input) throws IOException {
        int size = input.readInt();
        splits = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            try {
                splits.add(JetStreamConsumerSplit.read(input));
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }
}
