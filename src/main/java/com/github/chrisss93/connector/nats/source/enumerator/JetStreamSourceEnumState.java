package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;

import java.util.Map;
import java.util.Set;

public class JetStreamSourceEnumState {
    private final Set<JetStreamConsumerSplit> assignedSplits;
    private final Map<Integer, Set<JetStreamConsumerSplit>> pendingAssignments;

    JetStreamSourceEnumState(
        Set<JetStreamConsumerSplit> assignedSplits, Map<Integer,
        Set<JetStreamConsumerSplit>> pendingAssignments) {

        this.assignedSplits = assignedSplits;
        this.pendingAssignments = pendingAssignments;
    }

    public Set<JetStreamConsumerSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<Integer, Set<JetStreamConsumerSplit>> getPendingAssignments() {
        return pendingAssignments;
    }

    @Override
    public String toString() {
        return "JetStreamSourceEnumState{" +
            "assignedSplits=" + getAssignedSplits() +
            ", pendingAssignments=" + getPendingAssignments() +
            '}';
    }
}