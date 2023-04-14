package com.github.chrisss93.connector.nats.testutils.source.cases;

import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.source.JetStreamSourceContext;

import java.util.Collection;
import java.util.Collections;

public class SingleFilterStreamContext extends JetStreamSourceContext {
    public SingleFilterStreamContext(NatsTestEnvironment runtime, String prefix) {
        super(runtime, prefix);
    }

    @Override
    public Collection<String> streamSubjectFilters() {
        return Collections.singleton(streamName);
    }

    @Override
    public String testDataToSubject(String testData) {
        return streamName;
    }
}
