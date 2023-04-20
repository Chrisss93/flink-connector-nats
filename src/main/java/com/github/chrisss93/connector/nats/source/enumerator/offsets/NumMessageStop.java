package com.github.chrisss93.connector.nats.source.enumerator.offsets;

import io.nats.client.Message;

public class NumMessageStop implements StopRule {


    private final long maxRecordNum;
    private long recordsSeen;

    public NumMessageStop(long maxRecordNum) {
        this.maxRecordNum = maxRecordNum;
    }

    @Override
    public boolean shouldStop(Message message) {
        recordsSeen++;
        return recordsSeen >= maxRecordNum;
    }
}
