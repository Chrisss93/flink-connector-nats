package com.github.chrisss93.connector.nats.testutils.sink;

import com.github.chrisss93.connector.nats.sink.JetStreamSink;
import com.github.chrisss93.connector.nats.sink.writer.serializer.StringSerializer;
import com.github.chrisss93.connector.nats.testutils.NATSTestContext;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import io.nats.client.Dispatcher;
import io.nats.client.Options;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV2ExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public abstract class JetStreamSinkContext extends NATSTestContext
    implements DataStreamSinkV2ExternalContext<String> {

    private static final int RECORD_SIZE_UPPER_BOUND = 300;
    private static final int RECORD_SIZE_LOWER_BOUND = 100;
    private static final int RECORD_STRING_SIZE = 50;
    private static final byte[] PUBLISH_ACK_BODY = "{\"stream\":\"\", \"seq\": 1}".getBytes(StandardCharsets.UTF_8);

    private transient Dispatcher dispatcher;
    private final ConcurrentLinkedQueue<String> readerQueue = new ConcurrentLinkedQueue<>();

    protected JetStreamSinkContext(NatsTestEnvironment runtime, String prefix) {
        super(runtime, prefix);
     }

    protected void sinkExtra(Properties props) {
    }

    protected abstract Collection<String> streamSubjectFilters();
    protected abstract String testDataToSubject(String testData);

    @Override
    public Sink<String> createSink(TestingSinkSettings sinkSettings) {
        dispatcher = runtime.client().createDispatcher(m -> {
            readerQueue.add(new String(m.getData(), StandardCharsets.UTF_8));
            runtime.client().publish(m.getReplyTo(), PUBLISH_ACK_BODY);
        });
        streamSubjectFilters().forEach(dispatcher::subscribe);

        Properties props = new Properties();
        props.setProperty(Options.PROP_URL, runtime.client().getConnectedUrl());
        sinkExtra(props);
        return new JetStreamSink<>(props, (StringSerializer) this::testDataToSubject);
    }

    @Override
    public ExternalSystemDataReader<String> createSinkDataReader(TestingSinkSettings sinkSettings) {
        return new ExternalSystemDataReader<String>() {
            public List<String> poll(Duration timeout) {
                return new ArrayList<>(readerQueue);
            }
            public void close() {}
        };
    }

    @Override
    public List<String> generateTestData(TestingSinkSettings sinkSettings, long seed) {
        Random random = new Random(seed);
        int recordSize = random.nextInt(RECORD_SIZE_UPPER_BOUND - RECORD_SIZE_LOWER_BOUND) +
            RECORD_SIZE_LOWER_BOUND;
        List<String> records = new ArrayList<>(recordSize);
        for (int i = 0; i < recordSize; i++) {
            int size = random.nextInt(RECORD_STRING_SIZE) + RECORD_STRING_SIZE;
            String record = "index:" + i + "-data:" + randomAlphanumeric(size);
            records.add(record);
        }
        return records;
    }

    @Override
    public void close() {
        runtime.client().closeDispatcher(dispatcher);
    }
}
