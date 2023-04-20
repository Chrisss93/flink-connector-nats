package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.NATSConsumerConfig;
import com.github.chrisss93.connector.nats.source.event.RevokeSplitsEvent;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import com.github.chrisss93.connector.nats.testutils.NatsTestSuiteBase;
import io.nats.client.Options;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class JetStreamSourceEnumeratorTest extends NatsTestSuiteBase {
    private static final String streamName = JetStreamSourceEnumeratorTest.class.getSimpleName();
    private static final int NUM_SUBTASKS = 3;
    private static final int READER0 = 0;

    @Test
    public void initAssignReadersSplits() {
        MockSplitEnumeratorContext<JetStreamConsumerSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        JetStreamSourceEnumerator enumerator = createEnumerator(
            Stream.of("a", "b", "c").collect(Collectors.toSet()),
            false, context
        );

        // Register readers
        for (int i = 0; i < NUM_SUBTASKS; i++) {
            registerReader(context, enumerator, i);
        }

        // Assign splits evenly one to every reader
        enumerator.start();
        assertThatCode(context::runNextOneTimeCallable).doesNotThrowAnyException();
        List<SplitsAssignment<JetStreamConsumerSplit>> assignments = context.getSplitsAssignmentSequence();
        assertThat(assignments).hasSize(1);
        assertThat(assignments.get(0).assignment().keySet()).containsExactly(0, 1, 2);

        enumerator.close();
    }

    @Test
    public void initMoreSplitsThanReaders() {
        MockSplitEnumeratorContext<JetStreamConsumerSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        JetStreamSourceEnumerator enumerator = createEnumerator(
            Stream.of("a", "b", "c", "d").collect(Collectors.toSet()),
            false, context
        );

        // Register readers
        for (int i = 0; i < NUM_SUBTASKS; i++) {
            registerReader(context, enumerator, i);
        }

        enumerator.start();
        assertThatCode(context::runNextOneTimeCallable).doesNotThrowAnyException();
        List<SplitsAssignment<JetStreamConsumerSplit>> assignments = context.getSplitsAssignmentSequence();
        assertThat(assignments).hasSize(1);
        Map<Integer, List<JetStreamConsumerSplit>> assignment = assignments.get(0).assignment();

        assertThat(assignment).hasSize(3);
        assertThat(assignment.get(0)).hasSize(1);
        assertThat(assignment.get(1)).hasSize(2);
        assertThat(assignment.get(2)).hasSize(1);

        enumerator.close();
    }

    @Test
    public void addSplitsBack() {
        MockSplitEnumeratorContext<JetStreamConsumerSplit> context = new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
        JetStreamSourceEnumerator enumerator = createEnumerator(
            Stream.of("a", "b", "c").collect(Collectors.toSet()),
            false, context
        );

        // Register readers
        for (int i = 0; i < NUM_SUBTASKS; i++) {
            registerReader(context, enumerator, i);
        }

        enumerator.start();
        assertThatCode(context::runNextOneTimeCallable).doesNotThrowAnyException();
        List<SplitsAssignment<JetStreamConsumerSplit>> assignments = context.getSplitsAssignmentSequence();
        assertThat(assignments).hasSize(1);
        Map<Integer, List<JetStreamConsumerSplit>> assignment = assignments.get(0).assignment();

        context.unregisterReader(READER0);
        enumerator.addSplitsBack(assignment.get(READER0), READER0);
        // Expect no new assignments
        assertThat(context.getSplitsAssignmentSequence()).hasSize(1);
        registerReader(context, enumerator, READER0);
        // Now that the reader has recovered, enumerator should assign the split back.
        assertThat(context.getSplitsAssignmentSequence()).hasSize(2);
        assertThat(context.getSplitsAssignmentSequence().get(1).assignment().keySet()).containsExactly(READER0);

        enumerator.close();
    }

    @Test
    public void dynamicSplits() throws Exception {
        int parallelism = 2;
        String subject1 = "red";
        String subject2 = "green";
        String subject3 = "blue";
        String subject4 = "yellow";
        createStream(streamName, subject1, subject2, subject3, subject4);

        MockSplitEnumeratorContext<JetStreamConsumerSplit> context = new MockSplitEnumeratorContext<>(parallelism);
        JetStreamSourceEnumerator enumerator = createEnumerator(Collections.singleton("na"), true, context);

        // Register readers
        for (int i = 0; i < parallelism; i++) {
            registerReader(context, enumerator, i);
        }

        enumerator.start();
        assertThatCode(context::runNextOneTimeCallable).doesNotThrowAnyException();
        List<SplitsAssignment<JetStreamConsumerSplit>> assignments = context.getSplitsAssignmentSequence();
        assertThat(assignments).hasSize(1);
        Map<Integer, List<JetStreamConsumerSplit>> assignment = assignments.get(0).assignment();
        assertThat(assignment.keySet()).containsExactly(0, 1);

        assertThat(assignment.get(0)).hasSize(2);
        List<String> reader1Subjects = assignment.get(0)
            .stream()
            .map(s -> s.getConfig().getFilterSubject())
            .collect(Collectors.toList());

        assertThat(reader1Subjects)
            .hasSize(2)
            .containsAnyOf(subject1, subject2, subject3, subject4);

        assertThat(assignment.get(1).stream().map(s -> s.getConfig().getFilterSubject()))
            .hasSize(2)
            .doesNotContainAnyElementsOf(reader1Subjects)
            .containsAnyOf(subject1, subject2, subject3, subject4);

        deleteStream(streamName);
        enumerator.close();
    }

    @Test
    public void periodicSplitDiscovery() throws Exception {
        int parallelism = 2;
        String[] subjects = new String[]{"red", "green", "blue", "yellow"};
        createStream(streamName, subjects);

        MockSplitEnumeratorContext<JetStreamConsumerSplit> context = new MockSplitEnumeratorContext<>(parallelism);
        JetStreamSourceEnumerator enumerator = createEnumerator(Collections.singleton("na"), true, 500L, context);

        // Register readers
        for (int i = 0; i < parallelism; i++) {
            registerReader(context, enumerator, i);
        }

        enumerator.start();
        assertThat(context.getPeriodicCallables())
            .as("A periodic split discovery callable should have been scheduled")
            .hasSize(1);
        assertThatCode(context::runNextOneTimeCallable).doesNotThrowAnyException();

        assertThat(context.getSplitsAssignmentSequence()).hasSize(1);
        List<String> assigned = context
            .getSplitsAssignmentSequence()
            .stream()
            .flatMap(x -> x.assignment().values().stream().flatMap(Collection::stream))
            .map(x -> x.getConfig().getFilterSubject())
            .collect(Collectors.toList());

        assertThat(assigned).containsExactlyInAnyOrder(subjects);

        // Edit the stream's subject-filter configuration
        String[] newSubjects = new String[]{"green", "blue", "orange"};
        client().jetStreamManagement().updateStream(
            StreamConfiguration.builder()
                .name(streamName)
                .subjects(newSubjects)
                .build()
        );
        // Discover the new subject-filters
        assertThatCode(() -> context.runPeriodicCallable(0)).doesNotThrowAnyException();

        // Check for new split assignment for new subject-filter: orange
        assertThat(context.getSplitsAssignmentSequence()).hasSize(2);
        Map<Integer, List<JetStreamConsumerSplit>> assignment = context.getSplitsAssignmentSequence()
            .get(1)
            .assignment();

        assertThat(assignment)
            .hasSize(1)
            .extracting(x -> x.get(1).stream()
                .map(y -> y.getConfig().getFilterSubject())
                .collect(Collectors.toList()))
            .isEqualTo(Collections.singletonList("orange"));

        // Check that special source-events have been emitted for assigned but outdated splits: red and yellow
        assertThat(context.getSentSourceEvent()).hasSize(2);
        List<SourceEvent> events = context.getSentSourceEvent()
            .values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        assertThat(events).hasSize(2);

        assertThat(events.get(0))
            .extracting(e -> ((RevokeSplitsEvent) e).getSplits().stream()
                .map(s -> s.getConfig().getFilterSubject())
                .collect(Collectors.toList()))
            .isEqualTo(Collections.singletonList("yellow"));

        assertThat(events.get(1))
            .extracting(e -> ((RevokeSplitsEvent) e).getSplits().stream()
                .map(s -> s.getConfig().getFilterSubject())
                .collect(Collectors.toList()))
            .isEqualTo(Collections.singletonList("red"));

        deleteStream(streamName);
        enumerator.close();
    }


    private JetStreamSourceEnumerator createEnumerator(Set<String> splitNames,
                                                       boolean dynamicConsumer,
                                                       SplitEnumeratorContext<JetStreamConsumerSplit> context) {
        return createEnumerator(splitNames, dynamicConsumer, -1, context);
    }
    private JetStreamSourceEnumerator createEnumerator(Set<String> splitNames,
                                                       boolean dynamicConsumer,
                                                       long discoveryInterval,
                                                       SplitEnumeratorContext<JetStreamConsumerSplit> context) {
        Properties props = new Properties();
        props.setProperty(Options.PROP_URL, client().getConnectedUrl());

        Set<NATSConsumerConfig> configs = splitNames.stream().map(
            s -> new NATSConsumerConfig(new ConsumerConfiguration.Builder().name(s).build())
        ).collect(Collectors.toSet());

        return new JetStreamSourceEnumerator(
            props,
            streamName,
            configs,
            dynamicConsumer,
            discoveryInterval,
            Boundedness.CONTINUOUS_UNBOUNDED,
            context
        );
    }

    private void registerReader(
        MockSplitEnumeratorContext<JetStreamConsumerSplit> context,
        JetStreamSourceEnumerator enumerator,
        int readerId) {

        context.registerReader(new ReaderInfo(readerId, "testing location"));
        enumerator.addReader(readerId);
    }
}
