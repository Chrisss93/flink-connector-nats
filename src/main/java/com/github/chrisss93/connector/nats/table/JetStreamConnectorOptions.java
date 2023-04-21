package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.source.enumerator.offsets.StartRule;
import io.nats.client.Options;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JetStreamConnectorOptions {

    public static final String SUBJECT_FIELD = "nats_subject";

    public static final ConfigOption<List<String>> SERVER_URLS =
        ConfigOptions.key(Options.PROP_SERVERS)
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("A list of NATS server URIs");

    public static final ConfigOption<String> USERNAME =
        ConfigOptions.key(Options.PROP_USERNAME)
            .stringType()
            .noDefaultValue()
            .withDescription("Set the username for basic authentication.");

    public static final ConfigOption<String> PASSWORD =
        ConfigOptions.key(Options.PROP_PASSWORD)
            .stringType()
            .noDefaultValue()
            .withDescription("Set the password for basic authentication.");

    public static final ConfigOption<String> TOKEN =
        ConfigOptions.key(Options.PROP_TOKEN)
            .stringType()
            .noDefaultValue()
            .withDescription("Set the token for token-based authentication.");

    public static final ConfigOption<Map<String, String>> CONNECT_PROPS =
        ConfigOptions.key("connect.extra.props")
            .mapType()
            .defaultValue(new HashMap<>())
            .withDescription("Additional properties to set up the NATS connection.");

    public static final ConfigOption<Map<String, String>> CONSUMER_PROPS =
        ConfigOptions.key("consumer.extra.props")
            .mapType()
            .defaultValue(new HashMap<>())
            .withDescription("Additional properties to set up the NATS consumer(s).");

    public static final ConfigOption<StopRuleEnum> STOP_RULE =
        ConfigOptions.key("stop.rule")
            .enumType(StopRuleEnum.class)
            .defaultValue(StopRuleEnum.NeverStop)
            .withDescription("The default value will make the source consume forever in unbounded-mode");

    public static final ConfigOption<Long> STOP_VALUE =
        ConfigOptions.key("stop.value")
            .longType()
            .defaultValue(-1L)
            .withDescription("An optional number indicating the stream sequence number, timestamp or the max" +
                " received messages, after which the source will stop consuming from the NATS stream. This" +
                " depends on whether '" + STOP_RULE.key() + "' is either: " + StopRuleEnum.StreamSequenceStop +
                " or " + StopRuleEnum.TimestampStop + " or " + StopRuleEnum.NumMessageStop + ". This setting" +
                " may be overwritten if the table query includes a LIMIT clause to be pushed-down.");

    public static final ConfigOption<StartRule> START_RULE =
        ConfigOptions.key("start.rule")
            .enumType(StartRule.class)
            .defaultValue(StartRule.EARLIEST)
            .withDescription("The type of starting point used to begin consuming from the NATS stream");

    public static final ConfigOption<Long> START_VALUE =
        ConfigOptions.key("start.value")
            .longType()
            .defaultValue(-1L)
            .withDescription("An optional number indicating the stream sequence number or timestamp to start" +
                " consuming from the NATS Stream (depending on whether '" + START_RULE.key() + "' is either: " +
                StartRule.FROM_STREAM_SEQUENCE + " or " + StartRule.FROM_TIME + ")");


    public static final ConfigOption<String> STREAM_NAME =
        ConfigOptions.key("stream")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the NATS stream to read from");

    public static final ConfigOption<String> CONSUMER_PREFIX =
        ConfigOptions.key("consumer.prefix")
            .stringType()
            .noDefaultValue()
            .withDescription("An identifier for the NATS consumer(s) that will be created for this table source");

    public static final ConfigOption<List<String>> SUBJECT_FILTERS =
        ConfigOptions.key("subject.filters")
            .stringType()
            .asList()
            .defaultValues()
            .withDescription("An optional list of one or more subject filters to control which portions of the " +
                "NATS stream to read into the table source. If left empty, every subject-filter for the stream" +
                "is read. This setting may be overwritten if the table query has an applicable WHERE clause on" +
                "a field called 'subject', which can be pushed-down.");

    public static final ConfigOption<Long> SPLIT_DISCOVERY_MS =
        ConfigOptions.key("subject.filter.discovery")
            .longType()
            .defaultValue(-1L)
            .withDescription("Interval in milliseconds for the source to periodically discover and consume from" +
                " all new subject-filters added to the NATS stream. This is only enabled if '" +
                SUBJECT_FILTERS.key() + "' is left empty.");

    public static final ConfigOption<String> SINK_SUBJECT =
        ConfigOptions.key("sink.subject")
            .stringType()
            .noDefaultValue()
            .withDescription("The NATS subject to insert records into. If this is not set, a varchar metadata "+
                "field: '" + SUBJECT_FIELD + "' must be present in the table definition."
            );

    enum StopRuleEnum {
        NeverStop,
        LatestStop,
        StreamSequenceStop,
        TimestampStop,
        NumMessageStop,
    }
}
