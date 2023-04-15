package com.github.chrisss93.connector.nats.source.enumerator.offsets;

import io.nats.client.api.StreamInfo;


public class LatestStop extends StreamSequenceStop {

    public LatestStop() {
        super(-1);
    }

    @Override
    public StreamSequenceStop create(StreamInfo info) {
        return new StreamSequenceStop(info.getStreamState().getLastSequence());
    }
}
