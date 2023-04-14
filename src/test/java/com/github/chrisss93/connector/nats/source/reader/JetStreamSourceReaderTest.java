package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.JetStreamSource;
import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.source.reader.deserializer.StringDeserializer;
import com.github.chrisss93.connector.nats.testutils.NatsTestSuiteBase;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.StreamSequenceStop;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class JetStreamSourceReaderTest extends NatsTestSuiteBase {
    @BeforeEach
    void makeStream(TestInfo test) throws Exception {
        String streamName = sanitizeDisplay(test);
        createStream(streamName, streamName + ".>");
    }

    @AfterEach
    void deleteStream(TestInfo test) throws Exception {
        String streamName = sanitizeDisplay(test);
        deleteStream(streamName);
    }

    @ParameterizedTest(name = "consumeMessagesAndAck")
    @ValueSource(booleans = {true, false})
    void consumeMessagesAndAck(boolean ackEach, TestInfo test) throws Exception {
        int NUM_MESSAGES = 25;

        String streamName =  sanitizeDisplay(test);
        String consumerName = randomAlphabetic(10);
        JetStreamSourceReaderBase<String> reader = createReader(streamName, ackEach);

        reader.addSplits(singletonList(buildSplit(streamName, consumerName, ackEach)));
        reader.notifyNoMoreSplits();

        for (int i = 0; i < NUM_MESSAGES; i++) {
            client().publish(String.format("%s.%s.%d", streamName, consumerName, i), new byte[]{1});
        }

        TestingReaderOutput<String> output = new TestingReaderOutput<>();
        pollUntil(
            reader,
            output,
            () -> output.getEmittedRecords().size() == NUM_MESSAGES,
            "The output didn't poll enough records before timeout."
        );
        reader.snapshotState(100L);
        reader.notifyCheckpointComplete(100L);
        pollUntil(
            reader,
            output,
            () -> reader.getMessagesToAck().isEmpty(),
            "The offset commit did not finish before timeout."
        );

        // Send another message after checkpoint has been taken
        client().jetStream().publish(String.format("%s.%s.%d", streamName, consumerName, 0), new byte[]{1});
        reader.close();
        reader.notifyNoMoreSplits();

        // Confirm that messages received before checkpoint have been acknowledged
        ConsumerInfo info = client().jetStreamManagement().getConsumerInfo(streamName, consumerName);
        assertThat(info.getAckFloor().getStreamSequence()).isEqualTo(NUM_MESSAGES);
        assertThat(info.getDelivered().getStreamSequence()).isEqualTo(NUM_MESSAGES + 1);
    }

    @ParameterizedTest(name = "checkpointSubsumingContract")
    @ValueSource(booleans = {true, false})
    void checkpointSubsumingContract(boolean ackEach, TestInfo test) throws Exception {
        String streamName =  sanitizeDisplay(test);
        String consumer1 = "foo";
        String consumer2 = "bar";
        JetStreamSourceReaderBase<String> reader = createReader(streamName, ackEach);

        // Multiple splits this time
        List<JetStreamConsumerSplit> splits = Arrays.asList(
            buildSplit(streamName, consumer1, ackEach),
            buildSplit(streamName, consumer2, ackEach)
        );
        reader.addSplits(splits);
        assertThat(reader.getNumberOfCurrentlyAssignedSplits()).isEqualTo(splits.size());

        int NUM_MESSAGES = 3;
        for (JetStreamConsumerSplit split: splits) {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                client().publish(String.format("%s.%s.%d", split.getStream(), split.getName(), i), new byte[]{1});
            }
        }

        TestingReaderOutput<String> output = new TestingReaderOutput<>();
        long checkpointId = 0;
        // Create checkpoint after each message is received, but do not complete them.
        while(output.getEmittedRecords().size() < NUM_MESSAGES * 2) {
            if (reader.pollNext(output) == InputStatus.MORE_AVAILABLE) {
                checkpointId++;
                reader.snapshotState(checkpointId);
            }
        }


        // Check that messages have been delivered by NATS but not acknowledged.
        assertThat(reader.getMessagesToAck()).hasSize((int) checkpointId);
        for (JetStreamConsumerSplit split : splits) {
            ConsumerInfo info = client().jetStreamManagement().getConsumerInfo(split.getStream(), split.getName());
            assertThat(info.getNumAckPending()).isEqualTo(NUM_MESSAGES);
        }

        // Complete the last checkpoint, which should acknowledge the messages for all the previous checkpoints too
        long lastCheckpoint = checkpointId;
        assertThatCode(() -> reader.notifyCheckpointComplete(lastCheckpoint)).doesNotThrowAnyException();
        reader.close();

        // Check that all messages have been acknowledged across both splits
        assertThat(reader.getMessagesToAck()).isEmpty();
        for (JetStreamConsumerSplit split : splits) {
            ConsumerInfo info = client().jetStreamManagement().getConsumerInfo(split.getStream(), split.getName());
            assertThat(info.getNumAckPending()).isEqualTo(0);
        }
    }

    @Disabled("I don't know how to force a fetcher to be idle, wtf...")
    @ParameterizedTest(name = "checkpointWithoutActiveFetcher")
    @ValueSource(booleans = {true, false})
    void checkpointWithoutActiveFetcher(boolean ackEach, TestInfo test) throws Exception {
        String streamName =  sanitizeDisplay(test);
        String consumerName = randomAlphabetic(10);

        JetStreamSourceReaderBase<String> reader =
            (JetStreamSourceReaderBase<String>) createSourceConfig(streamName, ackEach)
                .setStoppingRule(new StreamSequenceStop(1))
                .build()
                .createReader(new TestingReaderContext());

        reader.addSplits(singletonList(buildSplit(streamName, consumerName, ackEach)));
        reader.notifyNoMoreSplits();

        client().publish(String.format("%s.%s.0", streamName, consumerName), new byte[]{1});

        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        InputStatus status;
        do {
            status = reader.pollNext(output);
        } while (status != InputStatus.NOTHING_AVAILABLE);
        pollUntil(
            reader,
            output,
            () -> reader.getNumAliveFetchers() == 0,
            "The split fetcher did not exit before timeout."
        );

        // Confirm NATS message has been delivered but not acknowledged.
        ConsumerInfo info = client().jetStreamManagement().getConsumerInfo(streamName, consumerName);
        assertThat(info.getNumPending()).isEqualTo(1);
        assertThat(info.getDelivered().getStreamSequence()).isEqualTo(1);
        assertThat(reader.getMessagesToAck()).hasSize(1);

        // Try to checkpoint after the split's fetcher has been marked idle and shutdown by the fetcherManager.
        assertThatCode(() -> {
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
        }).doesNotThrowAnyException();

        // fetcherManager should have recreated the fetcher during checkpoint and sent message acknowledgements
        info = client().jetStreamManagement().getConsumerInfo(streamName, consumerName);
        assertThat(info.getNumPending()).isEqualTo(0);
        assertThat(reader.getMessagesToAck()).hasSize(0);

        reader.close();
    }

    @ParameterizedTest(name = "checkpointWithNoSplitRecords")
    @ValueSource(booleans = {true, false})
    void checkpointWithNoSplitRecords(boolean ackEach, TestInfo test) throws Exception {
        String streamName =  sanitizeDisplay(test);
        String consumerName = randomAlphabetic(10);
        JetStreamSourceReaderBase<String> reader = createReader(streamName, ackEach);

        reader.addSplits(singletonList(buildSplit(streamName, consumerName, ackEach)));
        reader.notifyNoMoreSplits();

        assertThatCode(() -> {
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
        }).doesNotThrowAnyException();

        reader.close();
    }

    // Uncomment once Flink 1.17 is supported
    /*
    @Test
    void pauseAndResumeSplits(TestInfo test) throws Exception {
        int NUM_MESSAGES = 3;
        String streamName =  sanitizeDisplay(test);
        String consumer1 = randomAlphabetic(10);
        String consumer2 = randomAlphabetic(10);
        JetStreamSourceReaderBase<String> reader = createReader(streamName, false);

        // Multiple splits this time
        List<JetStreamConsumerSplit> splits = Arrays.asList(
            buildSplit(streamName, consumer1, false),
            buildSplit(streamName, consumer2, false)
        );
        reader.addSplits(splits);
        // Verify a fetcher has been instantiated for each split.
        assertThat(reader.getNumberOfCurrentlyAssignedSplits()).isEqualTo(splits.size());
        assertThat(reader.getNumAliveFetchers()).isEqualTo(splits.size());

        for (JetStreamConsumerSplit split: splits) {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                client().publish(String.format("%s.%s.%d", split.getStream(), split.getName(), i), new byte[]{1});
            }
        }

        TestingReaderOutput<String> output1 = new TestingReaderOutput<>();
        pollUntil(
            reader,
            output1,
            () -> output1.getEmittedRecords().size() == NUM_MESSAGES * 2,
            "Should have received " + NUM_MESSAGES + "from each split"
        );

        reader.pauseOrResumeSplits(Collections.singleton(splits.get(0)), Collections.emptySet());
        for (int i = 0; i < NUM_MESSAGES; i++) {
            client().publish(
                String.format("%s.%s.%d", splits.get(0).getStream(), splits.get(0).getName(), i),
                new byte[]{1}
            );
        }

        TestingReaderOutput<String> output2 = new TestingReaderOutput<>();
        pollUntil(
            reader,
            output2,
            () -> output2.getEmittedRecords().isEmpty(),
            "Should have received no records because the split is paused"
        );


        reader.pauseOrResumeSplits(Collections.singleton(splits.get(1)), Collections.singleton(splits.get(0)));
        client().publish(
            String.format("%s.%s.%d", splits.get(0).getStream(), splits.get(0).getName(), 1),
            new byte[]{1}
        );

        TestingReaderOutput<String> output3 = new TestingReaderOutput<>();
        pollUntil(
            reader,
            output3,
            () -> output3.getEmittedRecords().size() == NUM_MESSAGES + 1,
            "Should have received all records since the split was paused"
        );

        client().publish(
            String.format("%s.%s.%d", splits.get(1).getStream(), splits.get(1).getName(), 1),
            new byte[]{1}
        );
        pollUntil(
            reader,
            output3,
            () -> output3.getEmittedRecords().size() == NUM_MESSAGES + 1,
            "Should have received no new records since split 2 was paused when split 1 was resumed"
        );

        reader.close();
    }
     */

    private void pollUntil(
        JetStreamSourceReaderBase<String> reader,
        ReaderOutput<String> output,
        Supplier<Boolean> condition,
        String errorMessage) throws Exception {

        pollUntil(reader, output, condition, Duration.ofSeconds(30), errorMessage);
    }
    private void pollUntil(
        JetStreamSourceReaderBase<String> reader,
        ReaderOutput<String> output,
        Supplier<Boolean> condition,
        Duration timeout,
        String errorMessage) throws Exception {

        waitUtil(
            () -> {
                try {
                    reader.pollNext(output);
                } catch (Exception exception) {
                    throw new RuntimeException(
                        "Caught unexpected exception when polling from the reader",
                        exception);
                }
                return condition.get();
            },
            timeout,
            errorMessage);
    }

    private JetStreamSourceReaderBase<String> createReader(String streamName, boolean ackEach) {
        return (JetStreamSourceReaderBase<String>) createSourceConfig(streamName, ackEach)
            .build()
            .createReader(new TestingReaderContext());
    }

    private JetStreamSourceBuilder<String> createSourceConfig(String streamName, boolean ackEach) {
        return JetStreamSource.<String>builder()
            .setStream(streamName)
            .setServerURL(client().getConnectedUrl())
            .setDeserializationSchema(new StringDeserializer())
            .setDefaultConsumerConfiguration("default")
            .ackEachMessage(ackEach);
    }


    private JetStreamConsumerSplit buildSplit(String streamName, String consumerName, boolean ackEach) {
        ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder()
            .name(consumerName)
            .durable(consumerName)
            .filterSubject(String.format("%s.%s.*", streamName, consumerName));

        ConsumerConfiguration conf = builder.ackPolicy(ackEach ? AckPolicy.Explicit : AckPolicy.All).build();
        return new JetStreamConsumerSplit(streamName, conf);
    }
}
