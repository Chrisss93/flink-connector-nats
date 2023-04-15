package com.github.chrisss93.connector.nats.source.enumerator.offsets;

public enum StartRule {
    EARLIEST,
    LATEST,
    FROM_TIME,
    FROM_STREAM_SEQUENCE
}
