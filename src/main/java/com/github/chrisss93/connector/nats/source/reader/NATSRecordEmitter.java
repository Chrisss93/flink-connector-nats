package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.reader.deserializer.NATSMessageDeserializationSchema;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitState;
import io.nats.client.Message;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class NATSRecordEmitter<T> implements RecordEmitter<Message, T, JetStreamConsumerSplitState> {
    private final NATSMessageDeserializationSchema<T> deserializationSchema;
    private final boolean saveAcksInState;

    public NATSRecordEmitter(NATSMessageDeserializationSchema<T> deserializationSchema, boolean saveAcksInState) {
        this.deserializationSchema = deserializationSchema;
        this.saveAcksInState = saveAcksInState;
    }

    @Override
    public void emitRecord(Message element, SourceOutput<T> output,
                           JetStreamConsumerSplitState splitState) throws Exception {

        T record = deserializationSchema.deserialize(element);

        if (saveAcksInState) {
            splitState.addPendingAck(element);
            splitState.updateStreamSequence(element.metaData().streamSequence());
        }
        output.collect(record, element.metaData().timestamp().toInstant().toEpochMilli());
    }
}
