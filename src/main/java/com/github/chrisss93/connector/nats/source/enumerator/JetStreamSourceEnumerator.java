package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.common.SubjectUtils;
import com.github.chrisss93.connector.nats.source.NATSConsumerConfig;
import com.github.chrisss93.connector.nats.source.event.FinishedSplitsEvent;
import com.github.chrisss93.connector.nats.source.event.RevokeSplitsEvent;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.RandomStringUtils.random;

/**
 * Given a parallelism and number of splits, this enumerator assigns splits evenly across readers with no special
 * affinity between a particular split and a particular reader.
 */
public class JetStreamSourceEnumerator implements SplitEnumerator<JetStreamConsumerSplit, JetStreamSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceEnumerator.class);
    private static final long DISCOVERY_INIT_DELAY_MS = 10000L;
    private final Options connectOpts;
    private final String streamName;
    private final SplitEnumeratorContext<JetStreamConsumerSplit> context;
    private final Set<NATSConsumerConfig> consumerConfigs;
    private final boolean discoverSplits;
    private final long splitDiscoveryIntervalMs;
    private final Boundedness boundedness;

    private final Map<Integer, Set<JetStreamConsumerSplit>> assignedSplits = new HashMap<>();
    private final Map<Integer, Set<JetStreamConsumerSplit>> pendingSplitAssignments = new HashMap<>();
    private final Set<String> finishedSplits = new HashSet<>();

    public JetStreamSourceEnumerator(
        Properties connectProps,
        String stream,
        Set<NATSConsumerConfig> consumerConfigs,
        boolean dynamicConsumers,
        long filterDiscoveryIntervalMs,
        Boundedness boundedness,
        SplitEnumeratorContext<JetStreamConsumerSplit> context,
        JetStreamSourceEnumState restoredState
        ) {

        this(connectProps, stream, consumerConfigs, dynamicConsumers, filterDiscoveryIntervalMs, boundedness, context);
        pendingSplitAssignments.putAll(restoredState.getPendingAssignments());
        assignedSplits.putAll(restoredState.getAssignedSplits());
    }

    public JetStreamSourceEnumerator(
            Properties connectProps,
            String stream,
            Set<NATSConsumerConfig> consumerConfigs,
            boolean discoverSplits,
            long splitDiscoveryIntervalMs,
            Boundedness boundedness,
            SplitEnumeratorContext<JetStreamConsumerSplit> context) {

        this.streamName = stream;
        this.connectOpts = new Options.Builder(connectProps).build();
        this.consumerConfigs = consumerConfigs;
        this.discoverSplits = discoverSplits;
        this.boundedness = boundedness;
        this.splitDiscoveryIntervalMs = splitDiscoveryIntervalMs;
        this.context = context;

        if (consumerConfigs.size() < 1) {
            throw new IllegalArgumentException("At least 1 consumerConfigs entry is required");
        }
    }

    @Override
    public void start() {
        // Lookup fresh split assignments if the enumerator has not been restored from a checkpoint.
        if (assignedSplits.isEmpty()) {
            context.callAsync(this::makeOrLookupSplits, this::assignAllSplits);
        }
        if (discoverSplits && splitDiscoveryIntervalMs > 0) {
            LOG.info("Starting JetStreamSourceEnumerator for stream {} with subject-filter discovery " +
                    "interval of {} ms.", streamName, splitDiscoveryIntervalMs
            );
            context.callAsync(
                this::makeOrLookupSplits,
                this::assignAllSplits,
                DISCOVERY_INIT_DELAY_MS,
                splitDiscoveryIntervalMs
            );
        }
    }

    @Override
    public void addSplitsBack(List<JetStreamConsumerSplit> splits, int subtaskId) {
        splits.forEach(assignedSplits.get(subtaskId)::remove);
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

        if (discoverSplits) {
            ConsumerConfiguration.Builder config = ConsumerConfiguration.builder(
                consumerConfigs.iterator().next().build()
            );
            String prefix = config.build().getName();
            try (Connection connection = Nats.connect(connectOpts)) {
                configs = connection
                    .jetStreamManagement()
                    .getStreamInfo(streamName)
                    .getConfiguration()
                    .getSubjects()
                    .stream()
                    .map(s -> {
                        String fullName = prefix + "-" +
                            random(5, 0, 0, true, false, null, new Random(s.hashCode()));
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
        revokeOutdatedSplits(configs);
        preparePendingSplits(configs);
        assignPendingSplits(context.registeredReaders().keySet());
    }

    private void preparePendingSplits(List<JetStreamConsumerSplit> fetchedSplits) {
        int assigned = assignedSplits.values().stream().mapToInt(Set::size).sum();
        for (int i = 0; i < fetchedSplits.size(); i++) {
            int readerId = (assigned + i + 1) % context.currentParallelism();
            JetStreamConsumerSplit split = fetchedSplits.get(i);
            if (assignedSplits.values().stream().noneMatch(x -> x.contains(split))) {
                pendingSplitAssignments.computeIfAbsent(readerId, k -> new HashSet<>()).add(split);
            }
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
                incrementalAssignments.computeIfAbsent(readerId, k -> new ArrayList<>()).addAll(splits);
                assignedSplits.computeIfAbsent(readerId, k -> new HashSet<>()).addAll(splits);
            }
        }

        if (!incrementalAssignments.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(incrementalAssignments));
        }

        if (boundedness == Boundedness.BOUNDED && splitDiscoveryIntervalMs < 0) {
            readers.forEach(context::signalNoMoreSplits);
        }

        if (context.metricGroup() != null) { // Can remove null-guard in Flink 1.18 (via FLINK-21000)
            context.metricGroup().setUnassignedSplitsGauge(() ->
                pendingSplitAssignments.values().stream().mapToLong(Set::size).sum()
            );
        }
    }

    private void revokeOutdatedSplits(List<JetStreamConsumerSplit> splits) {
        for (Map.Entry<Integer, Set<JetStreamConsumerSplit>> e : assignedSplits.entrySet()) {
            Set<JetStreamConsumerSplit> revokes = Sets.difference(e.getValue(), new HashSet<>(splits));
            if (!revokes.isEmpty()) {
                LOG.info("Revoking outdated splits: {} from reader {}", revokes, e.getKey());
                context.sendEventToSourceReader(e.getKey(), new RevokeSplitsEvent(revokes));
            }
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof FinishedSplitsEvent)) {
            throw new UnsupportedOperationException(
                String.format("Reader %d emitted source event %s, which is not supported.", subtaskId, sourceEvent)
            );
        }
        Set<String> finished = ((FinishedSplitsEvent) sourceEvent).getSplitIds();
        assignedSplits.get(subtaskId).removeIf(x -> finished.contains(x.splitId()));
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
    public void close() {}

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }
}