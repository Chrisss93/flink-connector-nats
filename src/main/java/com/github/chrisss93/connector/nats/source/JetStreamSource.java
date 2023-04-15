package com.github.chrisss93.connector.nats.source;


import com.github.chrisss93.connector.nats.source.reader.JetStreamSourceReaderAllAcks;
import com.github.chrisss93.connector.nats.source.reader.JetStreamSourceReaderLastAck;
import com.github.chrisss93.connector.nats.source.reader.JetStreamSplitReader;
import com.github.chrisss93.connector.nats.source.reader.NatsRecordEmitter;
import com.github.chrisss93.connector.nats.source.reader.deserializer.NatsMessageDeserializationSchema;
import com.github.chrisss93.connector.nats.source.reader.fetcher.JetStreamSourceFetcherManager;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitSerializer;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.StopRule;
import com.github.chrisss93.connector.nats.source.enumerator.JetStreamSourceEnumState;
import com.github.chrisss93.connector.nats.source.enumerator.JetStreamSourceEnumStateSerializer;
import com.github.chrisss93.connector.nats.source.enumerator.JetStreamSourceEnumerator;
import io.nats.client.Message;
import io.nats.client.Options;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

public class JetStreamSource<OUT>
    implements Source<OUT, JetStreamConsumerSplit, JetStreamSourceEnumState>, ResultTypeQueryable<OUT> {

    private final Properties connectProps;
    private final NatsMessageDeserializationSchema<OUT> deserializationSchema;
    private final String stream;
    private final Set<NATSConsumerConfig> consumerConfigs;
    private final StopRule stopRule;
    private final boolean dynamicConsumers;
    private final boolean ackMessageOnCheckpoint;
    private final boolean ackEachMessage;
    private final int numFetcherThreads;

    public JetStreamSource(Properties connectProps,
                           NatsMessageDeserializationSchema<OUT> deserializationSchema,
                           String stream,
                           Set<NATSConsumerConfig> consumerConfigs,
                           StopRule stopRule,
                           boolean dynamicConsumers,
                           boolean ackMessageOnCheckpoint,
                           boolean ackEachMessage,
                           int numFetcherThreads) {
        this.connectProps = connectProps;
        this.deserializationSchema = deserializationSchema;
        this.stream = stream;
        this.consumerConfigs = consumerConfigs;
        this.stopRule = stopRule;
        this.dynamicConsumers = dynamicConsumers;
        this.ackMessageOnCheckpoint = ackMessageOnCheckpoint;
        this.ackEachMessage = ackEachMessage;
        this.numFetcherThreads = numFetcherThreads;
    }

    @Override
    public Boundedness getBoundedness() {
        return stopRule.boundedness();
    }

    @Override
    public SourceReader<OUT, JetStreamConsumerSplit> createReader(SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        Supplier<SplitReader<Message, JetStreamConsumerSplit>> splitReaderSupplier =
            () -> new JetStreamSplitReader(
                new Options.Builder(connectProps).turnOnAdvancedStats().build(),
                stopRule,
                readerContext.metricGroup()
            );

        if (ackEachMessage) {
            return new JetStreamSourceReaderAllAcks<>(
                elementsQueue,
                new JetStreamSourceFetcherManager(numFetcherThreads, elementsQueue, splitReaderSupplier),
                new NatsRecordEmitter<>(deserializationSchema, ackMessageOnCheckpoint),
                readerContext);
        } else {
            return new JetStreamSourceReaderLastAck<>(
                elementsQueue,
                new JetStreamSourceFetcherManager(numFetcherThreads, elementsQueue, splitReaderSupplier),
                new NatsRecordEmitter<>(deserializationSchema, ackMessageOnCheckpoint),
                readerContext);
        }
    }

    @Override
    public SplitEnumerator<JetStreamConsumerSplit, JetStreamSourceEnumState> createEnumerator(
            SplitEnumeratorContext<JetStreamConsumerSplit> enumContext) {
        return new JetStreamSourceEnumerator(
            connectProps,
            stream,
            consumerConfigs,
            dynamicConsumers,
            getBoundedness(),
            enumContext
        );
    }

    @Override
    public SplitEnumerator<JetStreamConsumerSplit, JetStreamSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<JetStreamConsumerSplit> enumContext,
            JetStreamSourceEnumState checkpoint) {
        return new JetStreamSourceEnumerator(
            connectProps,
            stream,
            consumerConfigs,
            dynamicConsumers,
            getBoundedness(),
            enumContext,
            checkpoint
        );
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public SimpleVersionedSerializer<JetStreamConsumerSplit> getSplitSerializer() {
        return JetStreamConsumerSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<JetStreamSourceEnumState> getEnumeratorCheckpointSerializer() {
        return JetStreamSourceEnumStateSerializer.INSTANCE;
    }

    public static <OUT> JetStreamSourceBuilder<OUT> builder() {
        return new JetStreamSourceBuilder<>();
    }
}
