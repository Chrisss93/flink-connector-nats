package com.github.chrisss93.connector.nats.source.enumerator.offsets;

import io.nats.client.Message;
import io.nats.client.api.StreamInfo;
import org.apache.flink.api.connector.source.Boundedness;

import java.io.Serializable;

public interface StopRule extends Serializable {

    default Boundedness boundedness() {
        return Boundedness.BOUNDED;
    }

    default StopRule create(StreamInfo info) {
        return this;
    }

    boolean shouldStop(Message message);
}
