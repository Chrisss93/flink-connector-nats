package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.NATSConsumerConfig;
import com.github.chrisss93.connector.nats.source.event.CompleteAllSplitsEvent;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.pulsar.shade.org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/**
 * Given a parallelism and number of splits, this enumerator assigns splits evenly across readers with no special
 * affinity between a particular split and a particular reader.
 */
public class JetStreamSourceEnumerator implements SplitEnumerator<JetStreamConsumerSplit, JetStreamSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceEnumerator.class);
    private final Options connectOpts;
    private final String streamName;
    private final SplitEnumeratorContext<JetStreamConsumerSplit> context;
    private final Set<NATSConsumerConfig> consumerConfigs;
    private final boolean dynamicConsumers;
    private final Boundedness boundedness;

    private final Set<JetStreamConsumerSplit> assignedSplits = new HashSet<>();
    private final Map<Integer, Set<JetStreamConsumerSplit>> pendingSplitAssignments = new HashMap<>();

    public JetStreamSourceEnumerator(
        Properties connectProps,
        String stream,
        Set<NATSConsumerConfig> consumerConfigs,
        boolean dynamicConsumers,
        Boundedness boundedness,
        SplitEnumeratorContext<JetStreamConsumerSplit> context,
        JetStreamSourceEnumState restoredState
        ) {

        this(connectProps, stream, consumerConfigs, dynamicConsumers, boundedness, context);
        pendingSplitAssignments.putAll(restoredState.getPendingAssignments());
        assignedSplits.addAll(restoredState.getAssignedSplits());
    }

    public JetStreamSourceEnumerator(
            Properties connectProps,
            String stream,
            Set<NATSConsumerConfig> consumerConfigs,
            boolean dynamicConsumers,
            Boundedness boundedness,
            SplitEnumeratorContext<JetStreamConsumerSplit> context) {

        this.streamName = stream;
        this.connectOpts = new Options.Builder(connectProps).build();
        this.consumerConfigs = consumerConfigs;
        this.dynamicConsumers = dynamicConsumers;
        this.boundedness = boundedness;
        this.context = context;

        if (consumerConfigs.size() < 1) {
            throw new IllegalArgumentException("At least 1 consumerConfigs entry is required");
        }
    }

    @Override
    public void start() {
        if (context.metricGroup() != null) { // Can remove null-guard in Flink 1.18 (via FLINK-21000)
            context.metricGroup().setUnassignedSplitsGauge(() ->
                pendingSplitAssignments.values().stream().mapToLong(Set::size).sum()
            );
        }
        // Lookup fresh split assignments if the enumerator has not been restored from a checkpoint.
        if (assignedSplits.isEmpty()) {
            context.callAsync(this::makeOrLookupSplits, this::assignAllSplits);
        }
    }

    @Override
    public void addSplitsBack(List<JetStreamConsumerSplit> splits, int subtaskId) {
        splits.forEach(assignedSplits::remove);
        preparePendingSplits(splits);
        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        assignPendingSplits(Collections.singleton(subtaskId));
    }


    private List<JetStreamConsumerSplit> makeOrLookupSplits() throws Exception {
        Stream<? extends ConsumerConfiguration.Builder> configs = consumerConfigs.stream();

        if (dynamicConsumers) {
            ConsumerConfiguration.Builder config = consumerConfigs.iterator().next();
            String prefix = config.build().getName();
            try (Connection connection = Nats.connect(connectOpts)) {
                configs = connection
                    .jetStreamManagement()
                    .getStreamInfo(streamName)
                    .getConfiguration()
                    .getSubjects()
                    .stream()
                    .map(s -> {
                        String fullName = prefix + "-" + randomAlphabetic(5);
                        return config.filterSubject(s).name(fullName).durable(fullName);
                    });
            }
        }
        return configs.map(c -> new JetStreamConsumerSplit(streamName, c.build())).collect(Collectors.toList());
    }

    private void assignAllSplits(List<JetStreamConsumerSplit> configs, Throwable throwable) {
        if (throwable != null) {
            throw new FlinkRuntimeException("Failed to fetch or prepare all configured NATS consumers ", throwable);
        }
        preparePendingSplits(configs);
        assignPendingSplits(context.registeredReaders().keySet());
    }

    private void preparePendingSplits(List<JetStreamConsumerSplit> fetchedConsumers) {
        for (int i = 0; i < fetchedConsumers.size(); i++) {
            int readerId = (assignedSplits.size() + i + 1) % context.currentParallelism();
            pendingSplitAssignments.computeIfAbsent(readerId, k -> new HashSet<>()).add(fetchedConsumers.get(i));
        }
    }

    // Same as KafkaSourceEnumerator#assignPendingPartitionSplits
    private void assignPendingSplits(Set<Integer> readers) {
        Map<Integer, List<JetStreamConsumerSplit>> incrementalAssignments =
            new HashMap<>(pendingSplitAssignments.size());

        for (int readerId : readers) {
            checkReaderRegistered(readerId);
            Set<JetStreamConsumerSplit> splits = pendingSplitAssignments.remove(readerId);
            if (splits != null) {
                LOG.info("Reader {} will be assigned splits: {}", readerId,
                    splits.stream().map(JetStreamConsumerSplit::splitId).collect(Collectors.toList())
                );
                System.out.printf("Reader %d will be assigned splits: %s%n", readerId,
                    splits.stream().map(JetStreamConsumerSplit::splitId).collect(Collectors.toList())
                );
                incrementalAssignments.computeIfAbsent(readerId, k -> new ArrayList<>()).addAll(splits);
                assignedSplits.addAll(splits);
            }
        }
        if (!incrementalAssignments.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(incrementalAssignments));
        }
        //if (boundedness == Boundedness.BOUNDED) {
        readers.forEach(context::signalNoMoreSplits);
        //}
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public JetStreamSourceEnumState snapshotState(long checkpointId) {
        return new JetStreamSourceEnumState(assignedSplits, pendingSplitAssignments);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (boundedness != Boundedness.BOUNDED) {
            return;
        } else if (!(sourceEvent instanceof CompleteAllSplitsEvent)) {
            throw new UnsupportedOperationException(
                String.format("source event %s from reader %s is not supported", sourceEvent, subtaskId)
            );
        }
        context.registeredReaders().forEach((k, v) -> {
            if (k != subtaskId) {
                LOG.info("Closing reader {} as reader {} has already finished.", k, subtaskId);
                context.sendEventToSourceReader(k, new CompleteAllSplitsEvent(subtaskId));
            }
        });
    }
}