package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.List

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

        Set<JetStreamConsumerSplit> assignedSplits = new HashSet<>();
        Map<Integer, JetStreamConsumerSplit> pendingSplitAssignments = new HashMap<>();

        assignedSplits.add(red);
        pendingSplitAssignments.put(2, green);
        pendingSplitAssignments.put(3, blue);

        JetStreamSourceEnumState myEnum = new JetStreamSourceEnumState(assignedSplits, pendingSplitAssignments);
        JetStreamSourceEnumStateSerializer serializer = new JetStreamSourceEnumStateSerializer();

        byte[] b = serializer.serialize(myEnum);
        JetStreamSourceEnumState deserializedEnum = serializer.deserialize(0, b);

        assertThat(deserializedEnum.getPendingSplitAssignments())
            .isEqualTo(myEnum.getPendingSplitAssignments());
        assertThat(deserializedEnum.getAssignedSplits())
            .containsExactlyInAnyOrderElementsOf(myEnum.getAssignedSplits());
    }

}
