package com.github.chrisss93.connector.nats.testutils.source;

import com.github.chrisss93.connector.nats.source.JetStreamSource;
import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.LatestStop;
import com.github.chrisss93.connector.nats.source.reader.deserializer.StringDeserializer;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.source.writer.JetStreamStringWriter;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamConfiguration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.pulsar.shade.org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public abstract class JetStreamSourceContext implements DataStreamSourceExternalContext<String> {
    private static final int BATCH_DATA_SIZE = 500;
    private final NatsTestEnvironment runtime;
    protected final String streamName;

    public abstract Collection<String> subjectFilters();
    public abstract String testDataToSubject(String testData);
    protected void builderExtra(JetStreamSourceBuilder<String> builder) {
    }

    public JetStreamSourceContext(NatsTestEnvironment runtime, String prefix) {
        this.runtime = runtime;
        this.streamName = String.format("%s_%s_%s", prefix, this.getClass().getSimpleName(), randomAlphanumeric(5));
    }

    @Override
    public Source<String, ?, ?> createSource(TestingSourceSettings sourceSettings) {
        JetStreamSourceBuilder<String> builder = JetStreamSource.<String>builder()
            .setServerURL(runtime.client().getConnectedUrl())
            .setDeserializationSchema(new StringDeserializer())
            .setStream(streamName)
            .setDefaultConsumerConfiguration("default");

        if (sourceSettings.getBoundedness() == Boundedness.BOUNDED) {
            builder.setStoppingRule(new LatestStop());
        }
        builderExtra(builder);
        return builder.build();
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(TestingSourceSettings sourceSettings) {
        makeInitStream();
        return new JetStreamStringWriter(runtime.client(), this::testDataToSubject);
    }

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

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    private void makeInitStream() {
        try {
            runtime.client().jetStreamManagement().addStream(
                new StreamConfiguration.Builder()
                    .name(streamName)
                    .subjects(subjectFilters())
                    .build()
            );
        } catch (IOException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        runtime.client().jetStreamManagement().deleteStream(streamName);
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
