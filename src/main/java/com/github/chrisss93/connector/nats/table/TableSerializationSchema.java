package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.sink.writer.serializer.NATSMessageSerializationSchema;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

public class TableSerializationSchema implements NATSMessageSerializationSchema<RowData> {

    private final String subject;
    private final int subjectFieldIndex;
    private final int headerFieldIndex;
    private final SerializationSchema<RowData> codec;

    public TableSerializationSchema(String subject, int subjectFieldIndex, int headerFieldIndex,
                                    SerializationSchema<RowData> codec) {
        this.subject = subject;
        this.subjectFieldIndex = subjectFieldIndex;
        this.headerFieldIndex = headerFieldIndex;
        this.codec = codec;
    }

    @Override
    public byte[] serialize(RowData row) {
        return codec.serialize(row);
    }

    @Override
    public String getSubject(RowData row) {
        if (subject != null) {
            return subject;
        }
        return row.getString(subjectFieldIndex).toString();
    }

    @Override
    public Message makeMessage(RowData row, Long timestamp) {
        return new NatsMessage(getSubject(row), "", makeHeaders(row), serialize(row));
    }

    private Headers makeHeaders(RowData row) {
        Headers headers = new Headers();
        if (headerFieldIndex >= 0) {
            MapData map = row.getMap(headerFieldIndex);
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();
            for (int i = 0; i < keys.size(); i++) {
                ArrayData headerValues = values.getArray(i);
                for (int j = 0; j < headerValues.size(); j++) {
                    headers.add(keys.getString(i).toString(), headerValues.getString(j).toString());
                }
            }
        }
        return headers;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        codec.open(context);
    }
}
