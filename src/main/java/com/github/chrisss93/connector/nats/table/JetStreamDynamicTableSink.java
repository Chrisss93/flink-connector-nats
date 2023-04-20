package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.sink.JetStreamSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.github.chrisss93.connector.nats.table.JetStreamConnectorOptions.SINK_SUBJECT;

public class JetStreamDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    private final Properties connectProps;
    private final String subject;
    private final DataType consumedDataType;
    private final Integer parallelism;
    private final EncodingFormat<SerializationSchema<RowData>> formatter;
    private int subjectFieldIndex = -1;
    private int headerFieldIndex = -1;

    public JetStreamDynamicTableSink(Properties connectProps,
                                     String subject,
                                     DataType consumedDataType,
                                     Integer parallelism,
                                     EncodingFormat<SerializationSchema<RowData>> formatter) {
        this.connectProps = connectProps;
        this.subject = subject;
        this.consumedDataType = consumedDataType;
        this.parallelism = parallelism;
        this.formatter = formatter;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        TableSerializationSchema serializationSchema = new TableSerializationSchema(
            subject, subjectFieldIndex, headerFieldIndex,
            formatter.createRuntimeEncoder(context, consumedDataType)
        );
        return SinkV2Provider.of(new JetStreamSink<>(connectProps, serializationSchema), parallelism);
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        if (subject == null && !metadataKeys.contains(SUBJECT_FIELD)) {
            throw new TableException("Since the config option: '" + SINK_SUBJECT.key() + "' is not set, " +
                "the following VARCHAR METADATA field must be present: " + SUBJECT_FIELD);
        }
        List<DataTypes.Field> fields = DataType.getFields(consumedDataType);
        for (int i = 0; i < fields.size(); i++) {
            DataTypes.Field field = fields.get(i);
            if (subject == null &&
                field.getName().equals(SUBJECT_FIELD) &&
                field.getDataType().equals(DataTypes.STRING())) {
                subjectFieldIndex = i;
            } else if (field.getName().equals(HEADER_FIELD) && field.getDataType().equals(HEADER_TYPE)) {
                headerFieldIndex = i;
            }
        }
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return METADATA;
    }

    static final String SUBJECT_FIELD = "sink_subject";
    private static final String HEADER_FIELD = "headers";
    private static final DataType HEADER_TYPE = DataTypes.MAP(
        DataTypes.STRING(),
        DataTypes.ARRAY(DataTypes.STRING())
    );
    private static final Map<String, DataType> METADATA;
    static {
        METADATA = new HashMap<>();
        METADATA.put(SUBJECT_FIELD, DataTypes.STRING());
        METADATA.put(HEADER_FIELD, HEADER_TYPE);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public String asSummaryString() {
        return "NATS table sink";
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }
}
