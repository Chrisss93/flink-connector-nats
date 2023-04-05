package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;

import java.util.Map;
import java.util.Set;

public class JetStreamSourceEnumState {
    private final Set<JetStreamConsumerSplit> assignedSplits;
    private final Map<Integer, JetStreamConsumerSplit> pendingSplitAssignments;

    JetStreamSourceEnumState(Set<JetStreamConsumerSplit> assignedSplits, Map<Integer, JetStreamConsumerSplit> pendingSplitAssignments) {
        this.assignedSplits = assignedSplits;
        this.pendingSplitAssignments = pendingSplitAssignments;
    }

    public Set<JetStreamConsumerSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<Integer, JetStreamConsumerSplit> getPendingSplitAssignments() {
        return pendingSplitAssignments;
    }

    @Override
    public String toString() {
        return "JetStreamSourceEnumState{" +
            "assignedSplits=" + getAssignedSplits() +
            ", pendingSPlits=" + getPendingSplitAssignments() +
            '}';
    }
}