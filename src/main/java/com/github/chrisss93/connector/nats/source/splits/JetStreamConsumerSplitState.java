package com.github.chrisss93.connector.nats.source.splits;

import io.nats.client.Message;
import io.nats.client.api.ConsumerConfiguration;

public interface JetStreamConsumerSplitState {
    void addPendingAck(Message msg);
    void setStartSequence(long i);
    JetStreamConsumerSplit toSplit();
}

abstract class JetStreamConsumerSplitStateImpl implements JetStreamConsumerSplitState {
    private final String stream;
    private final ConsumerConfiguration.Builder config;

    public JetStreamConsumerSplitStateImpl(JetStreamConsumerSplit split) {
        this.stream = split.getStream();
        this.config = new ConsumerConfiguration.Builder(split.getConfig());
    }

    @Override
    public void setStartSequence(long i) {
        this.config.startSequence(i);
    }

    public String getStream() {
        return stream;
    }
    public ConsumerConfiguration getConfig() {
        return config.build();
    }
}

