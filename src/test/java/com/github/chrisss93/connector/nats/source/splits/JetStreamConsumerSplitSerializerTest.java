package com.github.chrisss93.connector.nats.source.splits;

import io.nats.client.api.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class JetStreamConsumerSplitSerializerTest {

    @Test
    public void serializeDeserialize() throws IOException {
        ConsumerConfiguration conf = ConsumerConfiguration.builder()
            .name("bar")
            .filterSubject(".*")
            .replayPolicy(ReplayPolicy.Original)
            .ackPolicy(AckPolicy.All)
            .deliverPolicy(DeliverPolicy.ByStartSequence)
            .build();

        Set<String> acks = new HashSet<>(Arrays.asList("Z", "B", "C"));

        JetStreamConsumerSplit split = new JetStreamConsumerSplit("foo", conf, acks);
        JetStreamConsumerSplitSerializer serializer = new JetStreamConsumerSplitSerializer();

        byte[] b = serializer.serialize(split);
        JetStreamConsumerSplit deserializedSplit = serializer.deserialize(0, b);

        assertThat(deserializedSplit).isEqualTo(split);
    }
}
