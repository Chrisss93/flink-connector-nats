package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import com.google.common.collect.ImmutableSet;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class JetStreamSourceEnumStateSerializerTest {

    @Test
    public void serializeDeserialize() throws Exception {
        JetStreamConsumerSplit red = new JetStreamConsumerSplit("colour",
            ConsumerConfiguration.builder()
                .name("red").filterSubject("colour.red.*")
                .startSequence(10)
                .replayPolicy(ReplayPolicy.Original)
                .ackPolicy(AckPolicy.All)
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .build()
        );

        JetStreamConsumerSplit green = new JetStreamConsumerSplit("colour",
            ConsumerConfiguration.builder()
                .name("green").filterSubject("colour.green.*")
                .replayPolicy(ReplayPolicy.Instant)
                .ackPolicy(AckPolicy.None)
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .startTime(ZonedDateTime.now().withZoneSameInstant(ZoneId.of("GMT")))
                .build()
        );

        JetStreamConsumerSplit blue = new JetStreamConsumerSplit("colour",
            ConsumerConfiguration.builder()
                .name("blue").filterSubject("colour.blue.*")
                .replayPolicy(ReplayPolicy.Instant)
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.LastPerSubject)
                .build()
        );

        JetStreamConsumerSplit yellow = new JetStreamConsumerSplit("colour",
            ConsumerConfiguration.builder()
                .name("yellow").filterSubject("colour.yellow.*")
                .replayPolicy(ReplayPolicy.Instant)
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.Last)
                .build()
        );

        Set<JetStreamConsumerSplit> assignedSplits = new HashSet<>();
        Map<Integer, Set<JetStreamConsumerSplit>> pendingSplitAssignments = new HashMap<>();

        assignedSplits.add(red);
        pendingSplitAssignments.put(2, Collections.singleton(green));
        pendingSplitAssignments.put(3, Stream.of(blue, yellow).collect(Collectors.toSet()));

        JetStreamSourceEnumState myEnum = new JetStreamSourceEnumState(assignedSplits, pendingSplitAssignments);
        JetStreamSourceEnumStateSerializer serializer = new JetStreamSourceEnumStateSerializer();

        byte[] b = serializer.serialize(myEnum);
        JetStreamSourceEnumState deserializedEnum = serializer.deserialize(0, b);

        assertThat(deserializedEnum.getAssignedSplits())
            .containsExactlyInAnyOrderElementsOf(myEnum.getAssignedSplits());

        assertThat(deserializedEnum.getPendingAssignments().keySet())
            .isEqualTo(myEnum.getPendingAssignments().keySet());

        deserializedEnum.getPendingAssignments().forEach(
            (k, v) -> assertThat(v).containsExactlyInAnyOrderElementsOf(myEnum.getPendingAssignments().get(k))
        );
    }
}
