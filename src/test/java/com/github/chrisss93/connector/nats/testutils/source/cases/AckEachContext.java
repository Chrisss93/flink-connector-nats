package com.github.chrisss93.connector.nats.testutils.source.cases;

import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;

public class AckEachContext extends SingleStreamContext {
    public AckEachContext(NatsTestEnvironment runtime, String prefix) {
        super(runtime, prefix);
    }

    @Override
    protected JetStreamSourceBuilder<String> sourceExtra(JetStreamSourceBuilder<String> builder) {
        return builder.ackEachMessage(true);
    }
}
