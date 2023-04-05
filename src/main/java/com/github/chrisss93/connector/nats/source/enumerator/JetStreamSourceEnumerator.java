package com.github.chrisss93.connector.nats.source.enumerator;

import com.github.chrisss93.connector.nats.source.NATSConsumerConfig;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.pulsar.shade.org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/*
    This enumerator is designed for use with the ConsumerSourceReader and ConsumerSplitReader which each only accept
    a single split. This enumerator defensively shouldn't try to assign more than 1 split to it (hence the
    representation of a reader->split assignment is a 1:1 map instead of a 1:N map like in Kafka and Pulsar source
    enumerators.

    This may be overly defensive. Test, and if possible, relax the data-structure to the more generalizable form
    as Kafka/Pulsar source enumerators.
 */

// TODO: Check if stream filter-subjects have any wildcards. If they don't, the stream's individual subjects may be
//   treated as static and they can be evenly assigned to some configurable number of splits.

public class JetStreamSourceEnumerator implements SplitEnumerator<JetStreamConsumerSplit, JetStreamSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceEnumerator.class);
    private final Options connectOpts;
    private final String streamName;
    private final SplitEnumeratorContext<JetStreamConsumerSplit> context;
    private final Set<NATSConsumerConfig> consumerConfigs;
    private final boolean dynamicConsumers;

    private final Set<JetStreamConsumerSplit> assignedSplits = new HashSet<>();
    private final Map<Integer, JetStreamConsumerSplit> pendingSplitAssignments = new HashMap<>();

    public JetStreamSourceEnumerator(
            Properties connectProps,
            String stream,
            Set<NATSConsumerConfig> consumerConfigs,
            boolean dynamicConsumers,
            SplitEnumeratorContext<JetStreamConsumerSplit> context) {

        this.streamName = stream;
        this.connectOpts = new Options.Builder(connectProps).build();
        this.consumerConfigs = consumerConfigs;
        this.dynamicConsumers = dynamicConsumers;
        this.context = context;

        if (consumerConfigs.size() < 1) {
            throw new IllegalArgumentException("At least 1 consumerConfigs entry is required");
        }
    }

    @Override
    public void start() {
        context.callAsync(this::makeOrLookupSplits, this::assignAllSplits);
    }

    @Override
    public void addSplitsBack(List<JetStreamConsumerSplit> splits, int subtaskId) {
        addToPendingSplits(splits);
        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader {} to JetStreamSourceEnumerator for stream {}.", subtaskId, streamName);
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
                        String fullName = prefix + "-" + randomAlphabetic(15);
                        return config.filterSubject(s).name(fullName).durable(fullName);
                    });
            }
        }
        return configs.map(c -> new JetStreamConsumerSplit(streamName, c.build())).collect(Collectors.toList());
    }

    private void assignAllSplits(Collection<JetStreamConsumerSplit> configs, Throwable throwable) {
        if (throwable != null) {
            throw new FlinkRuntimeException("Failed to fetch or prepare all configured NATS consumers ", throwable);
        }
        addToPendingSplits(configs);
        assignPendingSplits(context.registeredReaders().keySet());
    }

    private void addToPendingSplits(Collection<JetStreamConsumerSplit> fetchedConsumers) {
        fetchedConsumers.stream()
                .sorted(Comparator.comparing(JetStreamConsumerSplit::getName))
                .forEachOrdered(c -> pendingSplitAssignments.put(pendingSplitAssignments.size() + 1, c));
    }

    private void assignPendingSplits(Set<Integer> readers) {
        for (int readerId : readers) {
            checkReaderRegistered(readerId);
            JetStreamConsumerSplit split = pendingSplitAssignments.remove(readerId);
            if (split != null) {
                LOG.info("Assigning split {} to reader {}", split.splitId(), readerId);
                System.out.printf("Assigning split %s to reader %d%n", split.splitId(), readerId);
                context.assignSplit(split, readerId);
                assignedSplits.add(split);
                // A reader will be assigned a single static split assignment
                 context.signalNoMoreSplits(readerId);
            }
        }
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
}