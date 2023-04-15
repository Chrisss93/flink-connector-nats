package com.github.chrisss93.connector.nats.source.splits;

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import java.util.Collections;

public class SplitsRemoval<SplitT> extends SplitsAddition<SplitT> {
    public SplitsRemoval() {
        super(Collections.emptyList());
    }
}
