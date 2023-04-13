package com.github.chrisss93.connector.nats.source.splits;

import io.nats.client.Message;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

public interface JetStreamConsumerSplitState {
    void addPendingAck(Message msg);
    void updateStreamSequence(long i);
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
    public void updateStreamSequence(long i) {
        this.config.startSequence(i + 1);
        this.config.deliverPolicy(DeliverPolicy.ByStartSequence);
    }

    public String getStream() {
        return stream;
    }
    public ConsumerConfiguration getConfig() {
        return config.build();
    }
}

