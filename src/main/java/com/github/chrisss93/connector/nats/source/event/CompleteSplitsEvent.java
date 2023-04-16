package com.github.chrisss93.connector.nats.source.event;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Collections;
import java.util.Set;

public class CompleteSplitsEvent implements SourceEvent {
    private final Set<JetStreamConsumerSplit> splitIds;

    public CompleteSplitsEvent() {
        this(Collections.emptySet());
    }
    public CompleteSplitsEvent(Set<JetStreamConsumerSplit> splitIds) {
        this.splitIds = splitIds;
    }

    public Set<JetStreamConsumerSplit> getSplits() {
        return splitIds;
    }
    public boolean allSplits() {
        return splitIds.isEmpty();
    }
}
