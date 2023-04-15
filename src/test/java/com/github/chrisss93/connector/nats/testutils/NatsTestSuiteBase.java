package com.github.chrisss93.connector.nats.testutils;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamConfiguration;
import org.apache.flink.util.TestLoggerExtension;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

@ExtendWith(TestLoggerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class NatsTestSuiteBase {

    @RegisterExtension
    static final NatsTestEnvironment environment = new NatsTestEnvironment();

    protected Connection client() {
        return environment.client();
    }


    protected void createStream(StreamConfiguration conf) throws IOException, JetStreamApiException {
        client().jetStreamManagement().addStream(conf);
    }
    protected void createStream(String streamName, String... subjectFilter) throws IOException, JetStreamApiException {
        createStream(new StreamConfiguration.Builder().name(streamName).subjects(subjectFilter).build());
    }
    protected void deleteStream(String streamName) throws IOException, JetStreamApiException {
        client().jetStreamManagement().deleteStream(streamName);
    }

    protected String sanitizeDisplay(TestInfo info) {
        return info.getDisplayName().replaceAll("[^A-Za-z0-9]","");
    }

}
