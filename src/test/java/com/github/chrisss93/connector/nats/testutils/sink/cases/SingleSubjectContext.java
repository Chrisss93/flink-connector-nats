package com.github.chrisss93.connector.nats.testutils.sink.cases;

import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.sink.JetStreamSinkContext;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

public class SingleSubjectContext extends JetStreamSinkContext {

    public SingleSubjectContext(NatsTestEnvironment runtime, String prefix) {
        super(runtime, prefix);
    }

    @Override
    protected Collection<String> streamSubjectFilters() {
        return Collections.singleton(streamName);
    }

    @Override
    protected String testDataToSubject(String testData) {
        return streamName;
    }
}
