package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitState;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import com.github.chrisss93.connector.nats.source.splits.LastAcksSplitState;
import io.nats.client.Message;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

public class JetStreamSourceReaderLastAck<T> extends JetStreamSourceReaderBase<T> {

    @Override
    protected JetStreamConsumerSplitState initializedState(JetStreamConsumerSplit split) {
        return new LastAcksSplitState(split);
    }

    public JetStreamSourceReaderLastAck(
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
        SplitFetcherManager<Message, JetStreamConsumerSplit> splitFetcherManager,
        RecordEmitter<Message, T, JetStreamConsumerSplitState> recordEmitter,
        SourceReaderContext context) {

        super(elementsQueue, splitFetcherManager, recordEmitter, true, context);
    }
}
