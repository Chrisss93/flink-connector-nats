package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.event.FinishedSplitsEvent;
import com.github.chrisss93.connector.nats.source.event.RevokeSplitsEvent;
import com.github.chrisss93.connector.nats.source.reader.fetcher.JetStreamSourceFetcherManager;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitState;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.Message;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * Depending on the NATS consumer configuration, NATS might not have a fixed-sized progress indicator to send to the
 * server (like a Kafka partition-offset), and we might need to acknowledge every individual consumed message to
 * advance the corresponding NATS consumer. This puts us in the situation of having an accumulating split-state,
 * where we must remember every single message until we acknowledge it.
 *
 * <p>
 * Therefore this class follows the pattern laid out in
 * {@link org.apache.flink.api.common.state.CheckpointListener CheckpointListener} class-docs for
 * <strong>Implementing Checkpoint Subsuming for Committing Artifacts</strong>.
 * Our "ready-set" is held by the assigned splits in {@link JetStreamConsumerSplit#getPendingAcks()} and its values are
 * added as references to our "pending-set": `messagesToAck` during checkpointing. On successful completion of a
 * checkpoint, the references in pending-set are acknowledged back to the NATS server and are removed, which will
 * remove them from the split-state as well.
 * </p>
 */

public class JetStreamSourceReader<T>
    extends SourceReaderBase<Message, T, JetStreamConsumerSplit, JetStreamConsumerSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceReader.class);
    private final SortedMap<Long, Map<String, Set<String>>> messagesToAck;
    private final Function<JetStreamConsumerSplit, JetStreamConsumerSplitState> initSplit;
    private final boolean doubleAck;
    private boolean receivedRevokeSplitsEvent = false;

    public JetStreamSourceReader(
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
        SplitFetcherManager<Message, JetStreamConsumerSplit> splitFetcherManager,
        RecordEmitter<Message, T, JetStreamConsumerSplitState> recordEmitter,
        Function<JetStreamConsumerSplit, JetStreamConsumerSplitState> initSplit,
        boolean doubleAck,
        SourceReaderContext context) {

        this(elementsQueue, splitFetcherManager, recordEmitter, initSplit, doubleAck, context.getConfiguration(),
            context);
    }
    public JetStreamSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
            SplitFetcherManager<Message, JetStreamConsumerSplit> splitFetcherManager,
            RecordEmitter<Message, T, JetStreamConsumerSplitState> recordEmitter,
            Function<JetStreamConsumerSplit, JetStreamConsumerSplitState> initSplit,
            boolean doubleAck,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.messagesToAck = Collections.synchronizedSortedMap(new TreeMap<>());
        this.initSplit = initSplit;
        this.doubleAck = doubleAck;
    }

    @Override
    public List<JetStreamConsumerSplit> snapshotState(long checkpointId) {
        List<JetStreamConsumerSplit> splits = super.snapshotState(checkpointId);

        Map<String, Set<String>> acks = messagesToAck.computeIfAbsent(checkpointId, id -> new HashMap<>());

        for (JetStreamConsumerSplit split: splits) {
            acks.computeIfAbsent(split.splitId(), x -> new HashSet<>()).addAll(split.getPendingAcks());
        }
//        If there are existing entries in messagesToAck, they are from prior checkpoints that did not complete.
//        Acks for newer checkpoints should contain all the acks for previous checkpoints and more. So we never
//        really need to keep acks for previous incomplete checkpoints. But it's useful at least to expose how
//        many checkpoints were incomplete for testing. And since the duplicate elements are all references
//        cleaning it up here vs. in notifyCheckpointComplete saves very little space anyhow.

//        messagesToAck.headMap(checkpointId).clear();
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        Map<String, Set<String>> acks = messagesToAck.remove(checkpointId);

        if (acks == null) {
            LOG.debug("Messages for checkpoint {} do not exist or have already been acked.", checkpointId);
            return;
        }
        ((JetStreamSourceFetcherManager) splitFetcherManager).acknowledgeMessages(acks, doubleAck);

        acks.forEach((k, v) -> v.clear());

//         Since each checkpoint entry in messagesToAck should be a superset of earlier incomplete checkpoints
//         and since their duplicate elements reference the same objects in the split, clearing the acks of the
//         newest completed checkpoint, should in-turn clear the acks from all previous failed checkpoints.
//         So we should be able to omit the following.

//        messagesToAck.headMap(checkpointId).forEach((k, v) -> v.forEach((x, y) -> y.clear()))

        messagesToAck.headMap(checkpointId).clear();
    }

    @Override
    protected JetStreamConsumerSplit toSplitType(String splitId, JetStreamConsumerSplitState splitState) {
        return splitState.toSplit();
    }

    @Override
    protected void onSplitFinished(Map<String, JetStreamConsumerSplitState> finishedSplitIds) {
        if (receivedRevokeSplitsEvent) {
            context.sendSourceEventToCoordinator(new FinishedSplitsEvent(finishedSplitIds.keySet()));
        }
        context.sendSplitRequest();
    }

    @Override
    protected JetStreamConsumerSplitState initializedState(JetStreamConsumerSplit split) {
        return initSplit.apply(split);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        receivedRevokeSplitsEvent = sourceEvent instanceof RevokeSplitsEvent;
        if (!receivedRevokeSplitsEvent) {
            throw new UnsupportedOperationException(String.format("source event: %s is not supported", sourceEvent));
        }
        RevokeSplitsEvent revokeEvent = (RevokeSplitsEvent) sourceEvent;
        ((JetStreamSourceFetcherManager) splitFetcherManager).signalSplitRevoke(revokeEvent);
    }

    @VisibleForTesting
    SortedMap<Long, Map<String, Set<String>>> getMessagesToAck() {
        return messagesToAck;
    }

    @VisibleForTesting
    int getNumAliveFetchers() {
        return splitFetcherManager.getNumAliveFetchers();
    }
}
