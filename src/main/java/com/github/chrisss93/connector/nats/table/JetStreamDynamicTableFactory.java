package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.common.SubjectUtils;
import com.github.chrisss93.connector.nats.source.JetStreamSource;
import com.github.chrisss93.connector.nats.source.JetStreamSourceBuilder;
import com.github.chrisss93.connector.nats.source.NATSConsumerConfig;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.*;
import io.nats.client.Options;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chrisss93.connector.nats.table.JetStreamConnectorOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

public class JetStreamDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "nats";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();

        JetStreamSourceBuilder<RowData> builder = JetStreamSource.<RowData>builder()
            .setConnectionProperties(options.get(CONNECT_PROPS))
            .setServerURLs(options.get(SERVER_URLS).toArray(new String[1]))
            .setStream(options.get(STREAM_NAME))
            .setStartingRule(options.get(START_RULE))
            .setStartingValue(options.get(START_VALUE));

        List<String> subjectFilters = options.get(SUBJECT_FILTERS);
        String prefix = options.get(CONSUMER_PREFIX);
        Map<String, String> consumerOpts = options.get(CONSUMER_PROPS);

        ConsumerConfiguration.Builder config;
        if (consumerOpts != null) {
            config = NATSConsumerConfig.of(consumerOpts);
        } else {
            config = new ConsumerConfiguration.Builder().name(prefix);
        }

        if (subjectFilters.isEmpty()) {
            builder
                .setDefaultConsumerConfiguration(config)
                .setSplitDiscoveryInterval(options.get(SPLIT_DISCOVERY_MS));
        } else {
            subjectFilters.forEach(s -> {
                String fullName = SubjectUtils.consumerName(prefix, s);
                ConsumerConfiguration conf = new ConsumerConfiguration.Builder(config.build())
                    .filterSubject(s)
                    .name(fullName)
                    .durable(fullName)
                    .build();
                builder.addConsumerConfiguration(conf);
            });
        }

        switch (options.get(STOP_RULE)) {
            case NeverStop:
                builder.setStoppingRule(new NeverStop());
                break;
            case LatestStop:
                builder.setStoppingRule(new LatestStop());
                break;
            case StreamSequenceStop:
                builder.setStoppingRule(new StreamSequenceStop(options.get(STOP_VALUE)));
                break;
            case TimestampStop:
                builder.setStoppingRule(new TimestampStop(options.get(STOP_VALUE)));
                break;
            case NumMessageStop:
                builder.setStoppingRule(new NumMessageStop(options.get(STOP_VALUE)));
                break;
        }

        DecodingFormat<DeserializationSchema<RowData>> format =
            helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate();

        return new JetStreamDynamicTableSource(
            builder,
            prefix,
            context.getPhysicalRowDataType(),
            format,
            context.getObjectIdentifier().asSummaryString()
        );
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();

        Properties connectProps = new Properties();
        connectProps.put(Options.PROP_SERVERS, String.join(",", options.get(SERVER_URLS)));
        connectProps.putAll(options.get(CONNECT_PROPS));

        String subject = options.get(SINK_SUBJECT);
        Integer parallelism = options.get(SINK_PARALLELISM);

        EncodingFormat<SerializationSchema<RowData>> format =
            helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate();

        return new JetStreamDynamicTableSink(
            connectProps,
            subject,
            context.getPhysicalRowDataType(),
            parallelism,
            format
        );
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return FORWARD_OPTIONS;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Arrays.asList(SERVER_URLS, FactoryUtil.FORMAT));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return FORWARD_OPTIONS;
    }

    private static final Set<ConfigOption<?>> FORWARD_OPTIONS;
    static {
        FORWARD_OPTIONS = Stream.of(
            STREAM_NAME, CONSUMER_PREFIX, SUBJECT_FILTERS, SPLIT_DISCOVERY_MS, SINK_SUBJECT,
            USERNAME, PASSWORD, TOKEN,
            STOP_RULE, STOP_VALUE, START_RULE, START_VALUE, CONNECT_PROPS, CONSUMER_PROPS,
            FactoryUtil.FORMAT
        ).collect(Collectors.toSet());
    }
}
