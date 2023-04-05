package com.github.chrisss93.connector.nats.source.splits;

import io.nats.client.Message;

import java.util.HashSet;
import java.util.Set;

public class LastAcksSplitState extends JetStreamConsumerSplitStateImpl {
    private String lastAck;

    public LastAcksSplitState(JetStreamConsumerSplit split) {
        super(split);
        if (split.getPendingAcks().size() > 1) {
            throw new IllegalArgumentException("LastAcksSplitState can only have a single pending acknowledgement");
        }
        this.lastAck = split.getPendingAcks().stream().findAny().orElse(null);
    }

    @Override
    public void addPendingAck(Message msg) {
        lastAck = msg.getReplyTo();
    }

    @Override
    public JetStreamConsumerSplit toSplit() {
        Set<String> pendingAcks = new HashSet<>();
        if (lastAck != null) {
            pendingAcks.add(lastAck);
        }
        return new JetStreamConsumerSplit(getStream(), getConfig(), pendingAcks);
    }
}
