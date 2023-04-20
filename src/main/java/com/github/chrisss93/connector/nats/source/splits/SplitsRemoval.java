package com.github.chrisss93.connector.nats.source.splits;

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import java.util.Collections;
import java.util.List;

public class SplitsRemoval<SplitT> extends SplitsAddition<SplitT> {

    public SplitsRemoval(List<SplitT> removals) {
        super(removals);
    }
    public SplitsRemoval() {
        this(Collections.emptyList());
    }
}
