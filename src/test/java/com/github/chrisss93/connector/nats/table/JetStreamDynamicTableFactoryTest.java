package com.github.chrisss93.connector.nats.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;

@Disabled
public class JetStreamDynamicTableFactoryTest {

    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String METADATA = "metadata";
    private static final String COMPUTED_COLUMN_NAME = "computed-column";


    private static final ResolvedSchema SCHEMA = new ResolvedSchema(
        Arrays.asList(
            Column.physical(NAME, DataTypes.STRING().notNull()),
            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
            Column.physical(TIME, DataTypes.TIMESTAMP(3)),
            Column.computed(COMPUTED_COLUMN_NAME,
                ResolvedExpressionMock.of(DataTypes.DECIMAL(10, 3), COUNT + " + 1.0"))),
        singletonList(WatermarkSpec.of(TIME,
            ResolvedExpressionMock.of(DataTypes.TIMESTAMP(3), TIME + " - INTERVAL '5' SECOND"))),
        null);

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
        new ResolvedSchema(
            Arrays.asList(
                Column.physical(NAME, DataTypes.STRING()),
                Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                Column.metadata(TIME, DataTypes.TIMESTAMP(3), "timestamp", false),
                Column.metadata(
                    METADATA, DataTypes.STRING(), "value.metadata_2", false)),
            Collections.emptyList(),
            null);

    private static final DataType SCHEMA_DATA_TYPE = SCHEMA.toPhysicalRowDataType();


    @Test
    public void tableSource() {
        JetStreamDynamicTableSource source = (JetStreamDynamicTableSource) createTableSource(
            SCHEMA, getBasicSourceOptions());

//        Assertions.assertThat(source).isEqualTo();

        ScanTableSource.ScanRuntimeProvider provider = source.getScanRuntimeProvider(
            ScanRuntimeProviderContext.INSTANCE);

//        Assertions.assertThat()
    }

    @Test
    public void tableSink() {
        JetStreamDynamicTableSink sink = (JetStreamDynamicTableSink) createTableSink(
            SCHEMA, getBasicSinkOptions());

//        Assertions.assertThat(sink).isEqualTo();

        DynamicTableSink.SinkRuntimeProvider provider = sink.getSinkRuntimeProvider(
            new SinkRuntimeProviderContext(false));

//        Assertions.assertThat()
    }


    private static Map<String, String> getBasicSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // JetStream options
        tableOptions.put("connector", JetStreamDynamicTableFactory.IDENTIFIER);
        tableOptions.put("io.nats.client.servers", "nats://0.0.0.0:4222");
        tableOptions.put("stream", JetStreamDynamicTableFactoryTest.class.getName());
        tableOptions.put("subject.filter.discovery", String.valueOf(10000L));
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        String formatDelimiterKey = TestFormatFactory.IDENTIFIER + "." + TestFormatFactory.DELIMITER.key();
        String failOnMissingKey = TestFormatFactory.IDENTIFIER + "." + TestFormatFactory.FAIL_ON_MISSING.key();
        tableOptions.put(formatDelimiterKey, ",");
        tableOptions.put(failOnMissingKey, "true");
        return tableOptions;
    }

    private static Map<String, String> getBasicSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // JetStream options.
        tableOptions.put("connector", JetStreamDynamicTableFactory.IDENTIFIER);
        tableOptions.put("io.nats.client.servers", "placeholder");
        tableOptions.put("sink.subject", "placeholder");
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        String formatDelimiterKey = TestFormatFactory.IDENTIFIER + "." + TestFormatFactory.DELIMITER.key();
        tableOptions.put(formatDelimiterKey, ",");
        return tableOptions;
    }
}
