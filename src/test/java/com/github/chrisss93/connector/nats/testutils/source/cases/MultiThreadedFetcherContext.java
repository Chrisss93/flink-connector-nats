package com.github.chrisss93.connector.nats.testutils.source.cases;

import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;

public class MultiThreadedFetcherContext extends SingleFilterStreamContext {
    private final int numFetcherThreads;

    public MultiThreadedFetcherContext(NatsTestEnvironment runtime, String prefix, int numFetcherThreads) {
        super(runtime, prefix);
        this.numFetcherThreads = numFetcherThreads;
    }

    @Override
    protected void builderExtra(JetStreamSourceBuilder<String> builder) {
        builder.setNumFetcherThreads(numFetcherThreads);
    }
}
