package com.github.chrisss93.connector.nats.source.enumerator;

import static org.assertj.core.api.Assertions.assertThat;

//public class JetStreamSourceEnumeratorTest extends NatsTestSuiteBase {
//    private static final String streamName = JetStreamSourceEnumeratorTest.class.getSimpleName();
//    private static final int NUM_SUBTASKS = 3;
//    private static final int READER0 = 0;
//    private static final int READER1 = 1;
//    private static final int READER2 = 2;
//
//    @Test
//    void addSplitsBack(TestInfo test) throws Exception {
//
//        try (MockSplitEnumeratorContext<JetStreamConsumerSplit> context =
//             new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
//             JetStreamSourceEnumerator enumerator = createEnumerator(streamName, context)) {
//
//            // Simulate a reader failure.
//            context.unregisterReader(READER0);
//            enumerator.addSplitsBack(
//                context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
//                READER0
//            );
//            assertThat(context.getSplitsAssignmentSequence())
//                .as("The added back splits should have not been assigned")
//                .hasSize(3);
//
//            // Simulate a reader recovery.
//            registerReader(context, enumerator, READER0);
//            verifyAllReaderAssignments(context, preexistingTopics, 3 + 1);
//        }
//    }
//
//    private JetStreamSourceEnumerator createEnumerator(String stream,
//                                                       SplitEnumeratorContext<JetStreamConsumerSplit> context
//                                                       ) {
//        new JetStreamSourceConfiguration.Builder()
//            .setServerURL(client().getConnectedUrl())
//            .addConsumerConfiguration()
//            .build();
//
//        new JetStreamSourceEnumerator();
//    }
//
//    private void registerReader(
//        MockSplitEnumeratorContext<JetStreamConsumerSplit> context,
//        JetStreamSourceEnumerator enumerator,
//        int reader) {
//
//        context.registerReader(new ReaderInfo(reader, "testing location"));
//        enumerator.addReader(reader);
//    }
//
//    private void verifyAllReaderAssignments(
//        MockSplitEnumeratorContext<PulsarPartitionSplit> context,
//        Set<String> topics,
//        int expectedAssignmentSeqSize) {
//        assertThat(context.getSplitsAssignmentSequence()).hasSize(expectedAssignmentSeqSize);
//        // Merge the assignments into one
//        List<SplitsAssignment<PulsarPartitionSplit>> sequence =
//            context.getSplitsAssignmentSequence();
//        Map<Integer, Set<PulsarPartitionSplit>> assignments = new HashMap<>();
//
//        for (int i = 0; i < expectedAssignmentSeqSize; i++) {
//            Map<Integer, List<PulsarPartitionSplit>> assignment = sequence.get(i).assignment();
//            assignment.forEach(
//                (key, value) ->
//                    assignments.computeIfAbsent(key, k -> new HashSet<>()).addAll(value));
//        }
//
//        // Compare assigned partitions with desired partitions.
//        Set<TopicPartition> expectedTopicPartitions = getExpectedTopicPartitions(topics);
//        int actualSize = assignments.values().stream().mapToInt(Set::size).sum();
//        assertThat(actualSize).isEqualTo(expectedTopicPartitions.size());
//    }
//}
