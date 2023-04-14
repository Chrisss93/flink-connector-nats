package com.github.chrisss93.connector.nats.testutils.sink.cases;

import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import io.nats.client.Options;

import java.util.Properties;

public class SmallBufferContext extends SingleSubjectContext {

    final private long bufferSize;

    public SmallBufferContext(NatsTestEnvironment runtime, String prefix, int bufferSize) {
        super(runtime, prefix);
        this.bufferSize = bufferSize;
    }

    @Override
    protected void sinkExtra(Properties props) {
        props.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, String.valueOf(bufferSize));
    }
}
