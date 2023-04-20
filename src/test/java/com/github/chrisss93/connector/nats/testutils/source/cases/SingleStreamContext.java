package com.github.chrisss93.connector.nats.testutils.source.cases;

import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.source.JetStreamSourceContext;

public class SingleStreamContext extends JetStreamSourceContext {
    private int subjectIndex = 0;

    public SingleStreamContext(NatsTestEnvironment runtime, String prefix) {
        super(runtime, prefix);
    }

    @Override
    protected String addSubjectName() {
        String name = streamName + "." + subjectIndex;
        if (subjectIndex == 0) {
            makeStream(name);
        } else {
            updateStream(name);
        }
        subjectIndex++;
        return name;
    }
}
