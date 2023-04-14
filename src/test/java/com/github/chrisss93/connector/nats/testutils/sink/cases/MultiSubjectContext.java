package com.github.chrisss93.connector.nats.testutils.sink.cases;

import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.sink.JetStreamSinkContext;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultiSubjectContext extends JetStreamSinkContext {

    private final int subjectNum;

    public MultiSubjectContext(NatsTestEnvironment runtime, String prefix, int subjectNum) {
        super(runtime, prefix);
        this.subjectNum = subjectNum;
    }

    @Override
    protected Collection<String> streamSubjectFilters() {
        return IntStream.range(0, subjectNum).mapToObj(i -> streamName + "." + i).collect(Collectors.toList());
    }

    @Override
    protected String testDataToSubject(String testData) {
        return streamName + "." + Math.floorMod(testData.hashCode(), subjectNum);
    }
}
