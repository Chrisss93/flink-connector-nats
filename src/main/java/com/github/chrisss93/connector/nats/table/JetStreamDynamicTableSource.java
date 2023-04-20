package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.common.SubjectUtils;
import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.NumMessageStop;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class JetStreamDynamicTableSource implements ScanTableSource, SupportsReadingMetadata,
    SupportsWatermarkPushDown, SupportsLimitPushDown, SupportsFilterPushDown {

    private final JetStreamSourceBuilder<RowData> builder;
    private final String prefix;
    private final DecodingFormat<DeserializationSchema<RowData>> formatter;
    private final List<String> partitionKeys;
    private final String tableIdentifier;

    // Mutable properties for Table Source Abilities
    private DataType producedDataType;
    private final List<String> metadataKeys = new ArrayList<>();
    private WatermarkStrategy<RowData> watermark = WatermarkStrategy.noWatermarks();
    private long limit = -1L;
    private final Set<String> pushDownFilters = new HashSet<>();

    public JetStreamDynamicTableSource(JetStreamSourceBuilder<RowData> builder,
                                       String prefix,
                                       DataType physicalType,
                                       DecodingFormat<DeserializationSchema<RowData>> formatter,
                                       List<String> partitionKeys,
                                       String tableIdentifier) {
        this.builder = builder;
        this.prefix = prefix;
        this.producedDataType = physicalType;
        this.formatter = formatter;
        this.partitionKeys = partitionKeys;
        this.tableIdentifier = tableIdentifier;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TableDeserializationSchema deserializerSchema = new TableDeserializationSchema(
            formatter.createRuntimeDecoder(context, producedDataType),
            context.createTypeInformation(producedDataType),
            metadataKeys
        );
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext,
                                                         StreamExecutionEnvironment execEnv) {

                builder.setDeserializationSchema(deserializerSchema);
                if (limit > 0) {
                    builder.setStoppingRule(new NumMessageStop(limit));
                }
                if (!pushDownFilters.isEmpty()) {
                    pushDownSubjectFilters(builder, pushDownFilters);
                }
                return execEnv.fromSource(builder.build(), watermark, "JetStreamSource-" + tableIdentifier);
            }

            @Override
            public boolean isBounded() {
                return builder
                    .setDeserializationSchema(deserializerSchema)
                    .build()
                    .getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<ResolvedExpression> remaining = new ArrayList<>();
        SubjectVisitor visitor = new SubjectVisitor();

        for (ResolvedExpression filter : filters) {
            Collection<String> subjects = visitor.visit(filter);
            if (subjects != null) {
                pushDownFilters.addAll(subjects);
                accepted.add(filter);
            } else {
                remaining.add(filter);
            }
        }
        return Result.of(accepted, remaining);
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys.addAll(metadataKeys);
        this.producedDataType = producedDataType;
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return TableDeserializationSchema.MetadataHandler.CATALOG;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermark = watermarkStrategy;
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        JetStreamDynamicTableSource copy = new JetStreamDynamicTableSource(
            builder, prefix, producedDataType, formatter, partitionKeys, tableIdentifier
        );
        copy.metadataKeys.addAll(metadataKeys);
        copy.limit = limit;
        copy.watermark = watermark;
        copy.pushDownFilters.addAll(pushDownFilters);
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "NATS JetStream table source";
    }


    private void pushDownSubjectFilters(JetStreamSourceBuilder<?> builder, Set<String> filters) {
        ConsumerConfiguration.Builder config;
        if (builder.getDefaultConsumer() != null) {
            config = ConsumerConfiguration.builder(builder.getDefaultConsumer().build());
        } else {
            config = ConsumerConfiguration.builder();
        }

        builder.clearDefaultConsumerConfiguration();
        builder.clearConsumerConfigurations();

        for (String filter : filters) {
            String fullName = SubjectUtils.consumerName(prefix, filter);
            builder.addConsumerConfiguration(
                ConsumerConfiguration.builder(config.build())
                    .filterSubject(filter)
                    .name(fullName)
                    .durable(fullName)
                    .build()
            );
        }
    }
}
