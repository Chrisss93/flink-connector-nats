package com.github.chrisss93.connector.nats.source.reader.fetcher;


import com.github.chrisss93.connector.nats.source.reader.JetStreamSplitReader;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import com.github.chrisss93.connector.nats.source.splits.SplitsRemoval;
import io.nats.client.Message;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

public class JetStreamSourceFetcherManager extends SplitFetcherManager<Message, JetStreamConsumerSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceFetcherManager.class);

    private final int numFetchers;

    public JetStreamSourceFetcherManager(
            int numFetchers,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
            Supplier<SplitReader<Message, JetStreamConsumerSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
        this.numFetchers = numFetchers;
    }

    @Override
    public void addSplits(List<JetStreamConsumerSplit> splitsToAdd) {
        Map<SplitFetcher<Message, JetStreamConsumerSplit>, List<JetStreamConsumerSplit>> m = new HashMap<>();

        for (JetStreamConsumerSplit split : splitsToAdd) {
            m.computeIfAbsent(getOrCreateFetcher(split.splitId()), x -> new ArrayList<>()).add(split);
        }

        m.forEach((fetcher, splits) -> {
            fetcher.addSplits(splits);
            startFetcher(fetcher);
        });
    }

    public void acknowledgeMessages(Map<String, Set<String>> acksToCommit, boolean doubleAck) {
        if (acksToCommit.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Set<String>> e : acksToCommit.entrySet() ) {
            LOG.debug("Sending acks for {} messages back to {}.", e.getValue().size(), e.getKey());
            SplitFetcher<Message, JetStreamConsumerSplit> splitFetcher = getOrCreateFetcher(e.getKey());

            // TODO: Figure out why I can't get the fetcher to handle this task asynchronously as it's done in the kafka connector?
//            enqueueAcksTask(splitFetcher, e.getValue());

            ((JetStreamSplitReader) splitFetcher.getSplitReader()).acknowledge(e.getValue(), doubleAck);
        }
    }

    /*
    private void enqueueAcksTask(
            SplitFetcher<Message, JetStreamConsumerSplit> splitFetcher,
            Set<String> replyTos) {

        JetStreamConsumerSplitReader splitReader = (JetStreamConsumerSplitReader) splitFetcher.getSplitReader();
        splitFetcher.enqueueTask(new SplitFetcherTask() {
                @Override
                public boolean run() {
                    splitReader.acknowledge(replyTos);
                    return true;
                }
                @Override
                public void wakeUp() {}
        });
    }
     */

    public void signalCloseAllFetchers() {
        fetchers.forEach( (id, fetcher) -> {
            fetcher.enqueueTask(new SplitFetcherTask() {
                @Override
                public boolean run() {
                    fetcher.getSplitReader().handleSplitsChanges(new SplitsRemoval<>());
                    return true;
                }
                @Override
                public void wakeUp() {
                }
            });
        });
    }

    private SplitFetcher<Message, JetStreamConsumerSplit> getOrCreateFetcher(String splitId) {
        int fetcherId = Math.floorMod(splitId.hashCode(), numFetchers);
        SplitFetcher<Message, JetStreamConsumerSplit> fetcher = fetchers.get(fetcherId);
        if (fetcher == null) {
            fetcher = createSplitFetcher();
        }
        return fetcher;
    }
}
