package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitState;
import com.github.chrisss93.connector.nats.source.splits.AllAcksSplitState;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.Message;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

public class JetStreamSourceReaderAllAcks<T> extends JetStreamSourceReaderBase<T> {

    @Override
    protected JetStreamConsumerSplitState initializedState(JetStreamConsumerSplit split) {
        return new AllAcksSplitState(split);
    }

    public JetStreamSourceReaderAllAcks(
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
        SplitFetcherManager<Message, JetStreamConsumerSplit> splitFetcherManager,
        RecordEmitter<Message, T, JetStreamConsumerSplitState> recordEmitter,
        SourceReaderContext context) {

        super(elementsQueue, splitFetcherManager, recordEmitter, false, context);
    }
}
