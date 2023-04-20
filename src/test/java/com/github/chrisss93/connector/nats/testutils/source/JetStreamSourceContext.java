package com.github.chrisss93.connector.nats.testutils.source;

import com.github.chrisss93.connector.nats.source.JetStreamSource;
import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.NumMessageStop;
import com.github.chrisss93.connector.nats.source.reader.deserializer.StringDeserializer;
import com.github.chrisss93.connector.nats.testutils.NATSTestContext;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.source.writer.JetStreamStringWriter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public abstract class JetStreamSourceContext extends NATSTestContext
    implements DataStreamSourceExternalContext<String> {

    private static final int BATCH_DATA_SIZE = 500;

    public JetStreamSourceContext(NatsTestEnvironment runtime, String prefix) {
        super(runtime, prefix);
    }

    protected JetStreamSourceBuilder<String> sourceExtra(JetStreamSourceBuilder<String> builder) {
        return builder;
    }

    @Override
    public Source<String, ?, ?> createSource(TestingSourceSettings sourceSettings) {
        JetStreamSourceBuilder<String> builder = JetStreamSource.<String>builder()
            .setServerURL(runtime.client().getConnectedUrl())
            .setDeserializationSchema(new StringDeserializer())
            .setStream(streamName)
            .setDefaultConsumerConfiguration("default")
            .setSplitDiscoveryInterval(500L);

        if (sourceSettings.getBoundedness() == Boundedness.BOUNDED) {
            builder
                .setStoppingRule(new NumMessageStop(BATCH_DATA_SIZE))
                .setSplitDiscoveryInterval(-1);
        }
        return sourceExtra(builder).build();
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(TestingSourceSettings sourceSettings) {
        return new JetStreamStringWriter(runtime.client(), addSubjectName());
    }

    protected abstract String addSubjectName();

    @Override
    public List<String> generateTestData(TestingSourceSettings sourceSettings, int splitIndex, long seed) {
        Random random = new Random(seed);
        return IntStream.range(0, BATCH_DATA_SIZE)
            .boxed()
            .map(
                index -> {
                    int length = random.nextInt(20) + 1;
                    return "split:"
                        + splitIndex
                        + "-index:"
                        + index
                        + "-content:"
                        + randomAlphanumeric(length);
                })
            .collect(Collectors.toList());
    }
}
