package com.github.chrisss93.connector.nats.source.enumerator.offsets;

import io.nats.client.Message;

public class TimestampStop implements StopRule {

    private final long stopTime;

    public TimestampStop(long stopTime) {
        this.stopTime = stopTime;
    }

    @Override
    public boolean shouldStop(Message message) {
        return message.metaData().timestamp().toInstant().toEpochMilli() > stopTime;
    }
}
