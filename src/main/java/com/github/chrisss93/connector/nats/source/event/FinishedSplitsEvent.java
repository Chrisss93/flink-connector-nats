package com.github.chrisss93.connector.nats.source.event;

import org.apache.flink.api.connector.source.SourceEvent;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class FinishedSplitsEvent implements SourceEvent, Serializable {

    private Set<String> splitIds;

    public FinishedSplitsEvent(Set<String> splitIds) {
        this.splitIds = splitIds;
    }

    public Set<String> getSplitIds() {
        return splitIds;
    }

    private void writeObject(ObjectOutputStream output) throws IOException {
        output.writeInt(splitIds.size());
        for (String id : splitIds) {
            output.writeUTF(id);
        }
        output.flush();
    }

    private void readObject(ObjectInputStream input) throws IOException {
        int size = input.readInt();
        splitIds = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            splitIds.add(input.readUTF());
        }
    }
}
