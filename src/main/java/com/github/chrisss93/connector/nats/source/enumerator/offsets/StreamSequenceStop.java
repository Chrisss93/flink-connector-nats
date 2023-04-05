package com.github.chrisss93.connector.nats.source.enumerator.offsets;

import io.nats.client.Message;

public class StreamSequenceStop implements StopRule {

    protected long stopSequence;

    public StreamSequenceStop(long stopSequence) {
        this.stopSequence = stopSequence;
    }


    @Override
    public boolean shouldStop(Message message) {
        return message.metaData().streamSequence() > stopSequence;
    }
}
