package com.github.chrisss93.connector.nats.source.event;

import org.apache.flink.api.connector.source.SourceEvent;

public class CompleteAllSplitsEvent implements SourceEvent {

    private final int fromReader;
    public int getFromReader() {
        return fromReader;
    }

    public CompleteAllSplitsEvent(int fromReader) {
        this.fromReader = fromReader;
    }
    public CompleteAllSplitsEvent() {
        this(-1);
    }
}
