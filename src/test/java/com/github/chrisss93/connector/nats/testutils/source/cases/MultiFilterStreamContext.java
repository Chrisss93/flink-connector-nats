package com.github.chrisss93.connector.nats.testutils.source.cases;

import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.source.JetStreamSourceContext;

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultiFilterStreamContext extends JetStreamSourceContext {
    private final int numSplits;

    public MultiFilterStreamContext(NatsTestEnvironment runtime, String prefix, int numSplits) {
        super(runtime, prefix);
        this.numSplits = numSplits;
    }

    @Override
    public Collection<String> streamSubjectFilters() {
        return IntStream.range(0, numSplits).mapToObj(x -> streamName + "." + x).collect(Collectors.toList());
    }

    @Override
    public String testDataToSubject(String testData) {
        Matcher m = Pattern.compile("split:([0-9]+)-index:.*").matcher(testData);

        if (!m.find()) {
            throw new RuntimeException("Don't know how to extract the subject from the message: " + testData);
        }
        return streamName + "." + m.group(1);
    }
}
