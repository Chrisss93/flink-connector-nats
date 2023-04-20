package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.source.reader.deserializer.NATSMessageDeserializationSchema;
import io.nats.client.Message;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableDeserializationSchema implements NATSMessageDeserializationSchema<RowData> {

    private final DeserializationSchema<RowData> codec;
    private final TypeInformation<RowData> typeInfo;
    private final List<String> metadataKeys;

    public TableDeserializationSchema(DeserializationSchema<RowData> codec,
                                      TypeInformation<RowData> typeInfo,
                                      List<String> metadataKeys) {
        this.codec = codec;
        this.typeInfo = typeInfo;
        this.metadataKeys = metadataKeys;
    }

    @Override
    public RowData deserialize(Message message) throws IOException {
        RowData rows = codec.deserialize(message.getData());
        GenericRowData data = (GenericRowData) rows;
        for (int i = 0; i < metadataKeys.size(); i++) {
            data.setField(
                rows.getArity() - metadataKeys.size() + i,
                MetadataHandler.extract(metadataKeys.get(i), message)
            );
        }
        return data;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        codec.open(context);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }

    static class MetadataHandler {
        private static final String headerKey = "headers";
        private static final String streamKey = "stream";
        private static final String subjectKey = "subject";
        private static final String consumerKey = "consumer";
        private static final String domainKey = "domain";
        private static final String deliveredKey = "delivered";
        private static final String streamSeqKey = "streamSeq";
        private static final String consumerSeqKey = "consumerSeq";
        private static final String timestampKey = "timestamp";
        private static final String pendingKey = "pending";
        static Map<String, DataType>  CATALOG;
        static {
            CATALOG = new HashMap<>();
            CATALOG.put(headerKey, DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())));
            CATALOG.put(streamKey, DataTypes.STRING());
            CATALOG.put(subjectKey, DataTypes.STRING());
            CATALOG.put(consumerKey, DataTypes.STRING());
            CATALOG.put(domainKey, DataTypes.STRING());
            CATALOG.put(deliveredKey, DataTypes.BIGINT());
            CATALOG.put(streamSeqKey, DataTypes.BIGINT());
            CATALOG.put(consumerSeqKey, DataTypes.BIGINT());
            CATALOG.put(timestampKey, DataTypes.TIMESTAMP_WITH_TIME_ZONE(9));
            CATALOG.put(pendingKey, DataTypes.BIGINT());
        }

        private static Object extract(String key, Message msg) {
            switch (key) {
                case headerKey:
                    if (!msg.hasHeaders()) {
                        return new GenericMapData(new HashMap<StringData, ArrayData>());
                    }
                    return new GenericMapData(
                        msg.getHeaders()
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(
                                e -> StringData.fromString(e.getKey()),
                                e -> new GenericArrayData(
                                    e.getValue().stream()
                                        .map(StringData::fromString)
                                        .collect(Collectors.toList())
                                        .toArray(new StringData[1]))
                            ))
                    );
                case streamKey:
                    return StringData.fromString(msg.metaData().getStream());
                case subjectKey:
                    return StringData.fromString(msg.getSubject());
                case consumerKey:
                    return StringData.fromString(msg.metaData().getConsumer());
                case domainKey:
                    return StringData.fromString(msg.metaData().getDomain());
                case deliveredKey:
                    return msg.metaData().deliveredCount();
                case streamSeqKey:
                    return msg.metaData().streamSequence();
                case consumerSeqKey:
                    return msg.metaData().consumerSequence();
                case timestampKey:
                    return msg.metaData().timestamp();
                case pendingKey:
                    return msg.metaData().pendingCount();
            }
            throw new IllegalStateException("Unknown metadata field: " + key);
        }
    }
}
