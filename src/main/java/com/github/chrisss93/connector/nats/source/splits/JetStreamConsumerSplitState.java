package com.github.chrisss93.connector.nats.source.splits;

import io.nats.client.Message;

public interface JetStreamConsumerSplitState {
    void addPendingAck(Message msg);
    void updateStreamSequence(long i);
    JetStreamConsumerSplit toSplit();
}
