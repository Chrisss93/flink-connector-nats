package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.enumerator.offsets.*;
import com.github.chrisss93.connector.nats.testutils.NatsTestSuiteBase;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.Message;
import io.nats.client.Options;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;

public class JetStreamSplitReaderTest extends NatsTestSuiteBase {
    private static final String streamName = JetStreamSplitReaderTest.class.getSimpleName();
    private static final int NUM_MESSAGES = 3;

    @BeforeAll
    void makeStream() throws Exception {
        createStream(streamName, streamName + ".>");
    }

    @AfterAll
    void deleteStream() throws Exception {
        deleteStream(streamName);
    }

    @Test
    void pollMessageAfterTimeout(TestInfo test) {
        String consumerName = sanitizeDisplay(test);
        JetStreamSplitReader splitReader = jetStreamConsumer(new NeverStop());

        // Tell reader to create NATS consumer and fetch records from an empty stream
        addSplit(splitReader, consumerName);
        List<Message> messages = fetchMessages(splitReader);
        assertThat(messages).hasSize(0);

        // Publish a message to the stream
        byte[] testData = randomAlphabetic(20).getBytes();
        client().publish(String.format("%s.%s.%d", streamName, consumerName, 0), testData);

        // Fetch records again.
        messages = fetchMessages(splitReader);
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getData()).isEqualTo(testData);
    }

    @Test
    void wakeupUnblocksFetchWithoutException(TestInfo test) throws InterruptedException {
        String consumerName = sanitizeDisplay(test);
        JetStreamSplitReader splitReader = jetStreamConsumer(new NeverStop());
        addSplit(splitReader, consumerName);

        client().publish(String.format("%s.%s.%d", streamName, consumerName, 0), new byte[]{1});

        long wakeupAfterMs = 100L;
        long cushion = 100L;
        AtomicReference<Throwable> error = new AtomicReference<>();
        // Define a blocking fetch call in a different thread.
        Thread t =
            new Thread(
                () -> {
                    try {
                        Instant start = Instant.now();
                        List<Message> fetched = fetchMessages(splitReader);
                        assertThat(Instant.now())
                            .isBeforeOrEqualTo(start.plusMillis(wakeupAfterMs + cushion));
                        assertThat(fetched).hasSize(1);
                    } catch (Throwable e) {
                        error.set(e);
                    }
                },
                "testWakeUp-thread");

        // Allow split-reader to start its blocking fetch
        t.start();
        Thread.sleep(wakeupAfterMs);
        // Issue wakeup to split-reader while it is blocking
        splitReader.wakeUp();

        assertThat(error.get()).isNull();
        client().publish(String.format("%s.%s.%d", streamName, consumerName, 1), new byte[]{1});
        // Fetch records again after being woken up.
        List<Message> messages = fetchMessages(splitReader);
        assertThat(messages).hasSize(1);
    }

    @Test
    void finishedSplitByLatestStop(TestInfo test) {
        String consumerName = sanitizeDisplay(test);
        JetStreamSplitReader splitReader = jetStreamConsumer(new LatestStop());

        byte i;
        for (i = 0; i < NUM_MESSAGES; i++) {
            client().publish(String.format("%s.%s.%d", streamName, consumerName, i), new byte[]{i});
        }
        JetStreamConsumerSplit split = addSplit(splitReader, consumerName);
        for (; i < NUM_MESSAGES * 2; i++) {
            client().publish(String.format("%s.%s.%d", streamName, consumerName, i), new byte[]{i});
        }
        List<Message> messages = fetchMessages(splitReader, Collections.singleton(split.splitId()));
        assertThat(messages).hasSize(3);

        for (byte b = 0; b < messages.size(); b++) {
            assertThat(messages.get(b).getData()).isEqualTo(new byte[]{b});
        }
    }

    @Test
    void finishedSplitByTimestampStop(TestInfo test) throws Exception {
        String consumerName = sanitizeDisplay(test);

        byte i;
        for (i = 0; i < NUM_MESSAGES ; i++) {
            client().publish(String.format("%s.%s.%d", streamName, consumerName, i), new byte[]{i});
        }

        client().flush(Duration.ofSeconds(1));
        JetStreamSplitReader splitReader = jetStreamConsumer(new TimestampStop(System.currentTimeMillis()));
        Thread.sleep(200);

        for (; i < NUM_MESSAGES * 2; i++) {
            client().publish(String.format("%s.%s.%d", streamName, consumerName, i), new byte[]{i});
        }
        JetStreamConsumerSplit split = addSplit(splitReader, consumerName);

        List<Message> messages = fetchMessages(splitReader, Collections.singleton(split.splitId()));
        assertThat(messages).hasSize(NUM_MESSAGES + 1);

        for (byte b = 0; b < messages.size(); b++) {
            assertThat(messages.get(b).getData()).isEqualTo(new byte[]{b});
        }
    }


    @Test
    void finishedSplitByStreamSequenceStop(TestInfo test) throws Exception {
        String consumerName = sanitizeDisplay(test);
        createStream(consumerName, consumerName + ".>");
        JetStreamSplitReader splitReader = jetStreamConsumer(new StreamSequenceStop(NUM_MESSAGES));

        for (byte i = 0; i < NUM_MESSAGES * NUM_MESSAGES; i++) {
            client().publish(String.format("%s.%s.%d", consumerName, consumerName, i), new byte[]{i});
        }
        JetStreamConsumerSplit split = addSplit(splitReader, consumerName, consumerName);

        List<Message> messages = fetchMessages(splitReader, Collections.singleton(split.splitId()));
        deleteStream(consumerName);
        assertThat(messages).hasSize(NUM_MESSAGES);

        for (byte b = 0; b < messages.size(); b++) {
            assertThat(messages.get(b).getData()).isEqualTo(new byte[]{b});
        }
    }

    @Test
    void pauseAndResumeSplit(TestInfo test) {
        String consumerName = sanitizeDisplay(test);
        JetStreamSplitReader splitReader = jetStreamConsumer(new NeverStop());
        JetStreamConsumerSplit split = addSplit(splitReader, consumerName);

        // Pause the split
        splitReader.pauseOrResumeSplits(Collections.singleton(split), Collections.emptySet());
        for (int i = 0; i < NUM_MESSAGES ; i++) {
            client().publish(String.format("%s.%s.%d", streamName, consumerName, i), new byte[]{1});
        }

        assertThat(fetchMessages(splitReader)).isEmpty();

        for (int i = 0; i < NUM_MESSAGES ; i++) {
            client().publish(String.format("%s.%s.%d", streamName, consumerName, i), new byte[]{1});
        }
        // Resume the split
        splitReader.pauseOrResumeSplits(Collections.emptySet(), Collections.singleton(split));
        assertThat(fetchMessages(splitReader)).hasSize(NUM_MESSAGES * 2);
    }

    private JetStreamSplitReader jetStreamConsumer(StopRule stopRule) {
        return new JetStreamSplitReader(
            Options.builder().server(client().getConnectedUrl()).build(),
            stopRule
        );
    }

    private JetStreamConsumerSplit addSplit(JetStreamSplitReader reader, String consumerName) {
        return addSplit(reader, streamName, consumerName);
    }
    private JetStreamConsumerSplit addSplit(JetStreamSplitReader reader, String stream, String consumerName) {
        ConsumerConfiguration conf = ConsumerConfiguration.builder()
            .durable(consumerName)
            .name(consumerName)
            .filterSubject(String.format("%s.%s.*", stream, consumerName))
            .build();

        JetStreamConsumerSplit split = new JetStreamConsumerSplit(stream, conf);
        SplitsAddition<JetStreamConsumerSplit> addition = new SplitsAddition<>(singletonList(split));
        reader.handleSplitsChanges(addition);
        return split;
    }

    private List<Message> fetchMessages(JetStreamSplitReader reader) {
        return fetchMessages(reader, new HashSet<>());
    }
    private List<Message> fetchMessages(JetStreamSplitReader reader, Set<String> expectedFinished) {
        List<Message> messages = new ArrayList<>();
        Set<String> finishedSplits = new HashSet<>();

        RecordsWithSplitIds<Message> records = reader.fetch();
        if (records.nextSplit() != null) {
            Message msg;
            while ((msg = records.nextRecordFromSplit()) != null) {
                messages.add(msg);
            }
            finishedSplits.addAll(records.finishedSplits());
        }
        assertThat(finishedSplits).isEqualTo(expectedFinished);
        return messages;
    }
}
