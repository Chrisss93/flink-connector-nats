package com.github.chrisss93.connector.nats.source.splits;

import io.nats.client.Message;

import java.util.Set;

public class AllAcksSplitState extends JetStreamConsumerSplitStateImpl {
    private final Set<String> pendingAcks;

    public AllAcksSplitState(JetStreamConsumerSplit split) {
        super(split);
        pendingAcks = split.getPendingAcks();
    }

    @Override
    public void addPendingAck(Message m) {
        pendingAcks.add(m.getReplyTo());
    }

    @Override
    public JetStreamConsumerSplit toSplit() {
        return new JetStreamConsumerSplit(getStream(), getConfig(), pendingAcks);
    }
}
