package com.github.chrisss93.connector.nats.testutils.source.writer;

import io.nats.client.Connection;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

public class JetStreamStringWriter implements ExternalSystemSplitDataWriter<String> {

    private final Connection connection;
    private final Function<String, String> subjectNammer;

    public JetStreamStringWriter(Connection connection, Function<String, String> subject) {
        this.connection = connection;
        this.subjectNammer = subject;
    }

    @Override
    public void writeRecords(List<String> records) {
        records.forEach(r -> connection.publish(subjectNammer.apply(r), r.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
