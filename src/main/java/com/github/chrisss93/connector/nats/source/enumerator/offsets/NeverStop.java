package com.github.chrisss93.connector.nats.source.enumerator.offsets;

import io.nats.client.Message;
import org.apache.flink.api.connector.source.Boundedness;

public class NeverStop implements StopRule {
    @Override
    public boolean shouldStop(Message message) {
        return false;
    }

    @Override
    public Boundedness boundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }
}
