package com.github.chrisss93.connector.nats.testutils.sink.reader;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.testcontainers.shaded.org.bouncycastle.crypto.tls.CipherType.stream;

public class JetStreamStringReader implements ExternalSystemDataReader<String> {

    private static final int MAX_BATCH = 1000;

    private final JetStreamSubscription subscription;

    public JetStreamStringReader(Connection connection, Collection<String> subjects) throws IOException, JetStreamApiException {
        subscription = connection.jetStream().subscribe(subjects.iterator().next(),
            PullSubscribeOptions.builder()
//                .stream(stream)
                .stream(subjects.iterator().next())
                .name(subjects.iterator().next() + randomAlphanumeric(5))
                .configuration(ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build())
                .build()
        );
    }

    @Override
    public List<String> poll(Duration timeout) {
        return subscription.fetch(MAX_BATCH, timeout)
            .stream()
            .map(x -> new String(x.getData(), StandardCharsets.UTF_8))
            .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        subscription.unsubscribe();
    }
}
