package com.github.chrisss93.connector.nats.testutils.source.cases;

import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;

public class MultiThreadedFetcherContext extends SingleStreamContext {
    private final int numFetcherThreads;

    public MultiThreadedFetcherContext(NatsTestEnvironment runtime, String prefix, int numFetcherThreads) {
        super(runtime, prefix);
        this.numFetcherThreads = numFetcherThreads;
    }

    @Override
    protected void sourceExtra(JetStreamSourceBuilder<String> builder) {
        builder.setNumFetchersPerReader(numFetcherThreads);
    }
}
