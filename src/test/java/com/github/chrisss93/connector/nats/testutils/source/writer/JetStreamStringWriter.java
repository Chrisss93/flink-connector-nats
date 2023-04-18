package com.github.chrisss93.connector.nats.testutils.source.writer;

import io.nats.client.Connection;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JetStreamStringWriter implements ExternalSystemSplitDataWriter<String> {

    private final Connection connection;
    private final String subject;

    public JetStreamStringWriter(Connection connection, String subject) {
        this.connection = connection;
        this.subject = subject;
    }

    @Override
    public void writeRecords(List<String> records) {
        records.forEach(r -> connection.publish(subject, r.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public void close() {
    }
}
