package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.reader.deserializer.NatsMessageDeserializationSchema;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplitState;
import io.nats.client.Message;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class NatsRecordEmitter<T> implements RecordEmitter<Message, T, JetStreamConsumerSplitState> {
    private final NatsMessageDeserializationSchema<T> deserializationSchema;
    private final boolean saveAcksInState;

    public NatsRecordEmitter(NatsMessageDeserializationSchema<T> deserializationSchema, boolean saveAcksInState) {
        this.deserializationSchema = deserializationSchema;
        this.saveAcksInState = saveAcksInState;
    }

    @Override
    public void emitRecord(Message element, SourceOutput<T> output,
                           JetStreamConsumerSplitState splitState) throws Exception {

        T record = deserializationSchema.deserialize(element);

        if (saveAcksInState) {
            splitState.addPendingAck(element);
            splitState.setStartSequence(element.metaData().streamSequence());
        }
        output.collect(record, element.metaData().timestamp().toInstant().toEpochMilli());
    }
}
