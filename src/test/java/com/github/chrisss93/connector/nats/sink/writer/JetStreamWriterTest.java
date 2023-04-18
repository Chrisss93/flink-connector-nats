package com.github.chrisss93.connector.nats.sink.writer;

import com.github.chrisss93.connector.nats.sink.writer.serializer.NATSMessageSerializationSchema;
import com.github.chrisss93.connector.nats.sink.writer.serializer.StringSerializer;
import com.github.chrisss93.connector.nats.testutils.NatsTestSuiteBase;
import io.nats.client.*;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JetStreamWriterTest extends NatsTestSuiteBase {

    @BeforeEach
    void makeStream(TestInfo test) throws Exception {
        String streamName = sanitizeDisplay(test);
        createStream(streamName, streamName + ".*");
    }

    @AfterEach
    void deleteStream(TestInfo test) throws Exception {
        String streamName = sanitizeDisplay(test);
        deleteStream(streamName);
    }

    @Test
    public void writeSuccess(TestInfo test) throws Exception {
        String streamName = sanitizeDisplay(test);

        // Create writer
        Properties prop = new Properties();
        prop.setProperty(Options.PROP_URL, client().getConnectedUrl());

        TaskMailbox mailbox = new TaskMailboxImpl();

        JetStreamWriter<String> writer = new JetStreamWriter<>(
            prop,
            (StringSerializer) element -> streamName + '.' + element,
            new MockInitContext(mailbox)
        );
        assertThat(mailbox.hasMail()).isFalse();

        // Ask writer to write to NATS
        String body = "foo";
        writer.write(body, new MockSinkWriterContext(0, 10));

        // Mailbox executor has a pending task to check async publish response
        assertThat(mailbox.hasMail()).isTrue();
        assertThat(writer.getPendingMessages()).isEqualTo(1);

        // Verify that message has made it to NATS
        assertThat(
            client().jetStreamManagement().getStreamInfo(streamName).getStreamState().getLastSequence()
        ).isEqualTo(1);

        JetStreamSubscription sub = client().jetStream().subscribe(
            streamName + ".*",
            PullSubscribeOptions.DEFAULT_PULL_OPTS
        );
        Message message = sub.reader(1, 2).nextMessage(Duration.ofMillis(200));
        assertThat(message.getSubject()).isEqualTo(streamName + "." + body);
        assertThat(message.getData()).isEqualTo(body.getBytes(StandardCharsets.UTF_8));

        List<String> timestamp = message.getHeaders().get(NATSMessageSerializationSchema.timestampHeaderKey);
        assertThat(timestamp).isEqualTo(Collections.singletonList(String.valueOf(10)));

        writer.close();
    }

    @Test
    public void writeFail(TestInfo test) throws Exception {
        String streamName = sanitizeDisplay(test);

        // Create writer
        Properties prop = new Properties();
        prop.setProperty(Options.PROP_URL, client().getConnectedUrl());

        TaskMailbox mailbox = new TaskMailboxImpl();

        JetStreamWriter<String> writer = new JetStreamWriter<>(
            prop,
            (StringSerializer) element -> element, // This subject will not match any stream's subject-filter
            new MockInitContext(mailbox)
        );
        assertThat(mailbox.hasMail()).isFalse();

        // Ask writer to write to NATS
        String body = "foo";
        assertThatCode(
            () -> writer.write(body, new MockSinkWriterContext(0, 10))
        ).doesNotThrowAnyException();

        // Mailbox executor has a pending task to check async publish response
        assertThat(mailbox.hasMail()).isTrue();
        assertThat(writer.getPendingMessages()).isEqualTo(1);

        // Writer checks async-publish responses during checkpointing and should bubble up the exception.
        assertThrows(FlinkRuntimeException.class, () -> writer.flush(false));
        assertThat(mailbox.hasMail()).isFalse();
        assertThat(
            client().jetStreamManagement().getStreamInfo(streamName).getStreamState().getLastSequence()
        ).isEqualTo(0);

        writer.close();
    }

    @Test
    public void writeMessageOverflow(TestInfo test) throws Exception {
        String streamName = sanitizeDisplay(test);

        // Create writer
        Properties prop = new Properties();
        prop.setProperty(Options.PROP_URL, client().getConnectedUrl());
        prop.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, String.valueOf(2));

        TaskMailbox mailbox = new TaskMailboxImpl();

        JetStreamWriter<String> writer = new JetStreamWriter<>(
            prop,
            (StringSerializer) element -> streamName + "." + element,
            new MockInitContext(mailbox)
        );
        assertThat(mailbox.hasMail()).isFalse();

        // Write one message that NATS should accept, and second that NATS should reject
        String body1 = "foo";
        String body2 = "bar";
        writer.write(body1, new MockSinkWriterContext(0, 10)); // Good write
        writer.write(body1 + ".stuff", new MockSinkWriterContext(0, 10)); // Bad write

        // Mailbox executor has a pending task to check async publish response
        assertThat(mailbox.hasMail()).isTrue();
        assertThat(writer.getPendingMessages()).isEqualTo(2);

        // Message overflow will force evaluation of pending publish async responses until we're under the limit
        // Since we are 1 over the limit, the first (good) write response is evaluated, not the second (bad) write
        // response, which would throw.
        assertThatCode(
            () -> writer.write(body2, new MockSinkWriterContext(0, 10))
        ).doesNotThrowAnyException();
        assertThat(mailbox.drain().size()).isEqualTo(2);
        assertThat(writer.getPendingMessages()).isEqualTo(2);

        writer.close();

        // Check results on NATS
        List<Message> messages = client().jetStream()
            .subscribe(streamName + ".*", PullSubscribeOptions.DEFAULT_PULL_OPTS)
            .fetch(256, Duration.ofMillis(200));

        // The two good writes make it to NATS, the second bad write does not.
        assertThat(
            messages.stream().map(m -> new String(m.getData(), StandardCharsets.UTF_8))
        ).containsExactlyElementsOf(
            Stream.of(body1, body2).collect(Collectors.toList())
        );
    }

    private static class MockInitContext implements Sink.InitContext {

        private final SinkWriterMetricGroup metricGroup;
        private final ProcessingTimeService timeService;
        private final TaskMailbox taskMailbox;

        MockInitContext(TaskMailbox taskMailbox) {
            this.taskMailbox = taskMailbox;
            this.timeService = new TestProcessingTimeService();

            OperatorIOMetricGroup ioMetricGroup = createUnregisteredOperatorMetricGroup().getIOMetricGroup();
            MetricGroup group = new MetricListener().getMetricGroup();
            this.metricGroup = InternalSinkWriterMetricGroup.mock(group, ioMetricGroup);
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }
        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new MailboxExecutorImpl(taskMailbox, -1, StreamTaskActionExecutor.IMMEDIATE);
        }
        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }
        @Override
        public int getSubtaskId() {
            return 0;
        }
        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }
        public int getAttemptNumber() {
            return 0;
        }
        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }
        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }
        @Override
        public SerializationSchema.InitializationContext
        asSerializationSchemaInitializationContext() {
            return new SerializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return metricGroup;
                }
                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return null;
                }
            };
        }
    }

    private static class MockSinkWriterContext implements SinkWriter.Context {
        private final long watermark;
        private final long timestamp;

        MockSinkWriterContext(long watermark, long timestamp) {
            this.watermark = watermark;
            this.timestamp = timestamp;
        }

        @Override
        public long currentWatermark() {
            return watermark;
        }
        @Override
        public Long timestamp() {
            return timestamp;
        }
    }
}
