package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.testutils.NatsTestSuiteBase;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.Headers;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;

public class JetStreamDynamicTableSinkITCase extends NatsTestSuiteBase {

    private static StreamTableEnvironment tEnv;
    private static final byte[] PUBLISH_ACK_BODY = "{\"stream\":\"\", \"seq\": 1}".getBytes(UTF_8);
    private static final String jsonTemplate = "{\"colour\":\"%s\"}";
    private static final String[] colours = new String[]{"green", "red", "blue"};

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension();

    @BeforeEach
    public void beforeEach() {
        tEnv = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    }

    @Test
    public void staticSubject(TestInfo info) throws Exception {
        String tableName = sanitizeDisplay(info);
        tEnv.executeSql(String.join("\n",
            "CREATE TABLE " + tableName + " (",
            "colour VARCHAR,",
            "headers MAP<STRING, ARRAY<STRING>> METADATA",
            ") WITH (",
            "'connector' = 'nats',",
            "'format' = 'json',",
            "'sink.subject' = '" + tableName + "',",
            "'io.nats.client.servers' = '" + client().getConnectedUrl() + "'",
            ")"
        ));

        Subscription sub = client().subscribe(tableName);

        StringBuilder query = new StringBuilder("INSERT INTO " + tableName + " VALUES ");
        for (String colour : colours) {
            query.append(String.format("\n('%1$s', map['version', array['0', '1']]),", colour));
        }
        tEnv.executeSql(query.deleteCharAt(query.length() - 1).toString());

        for (String colour: colours) {
            Message msg = sub.nextMessage(1000L);
            client().publish(msg.getReplyTo(), PUBLISH_ACK_BODY);
            Assertions.assertThat(new String(msg.getData(), UTF_8)).isEqualTo(String.format(jsonTemplate, colour));
            Assertions.assertThat(msg.getHeaders())
                .isNotNull()
                .extracting(Headers::entrySet)
                .isEqualTo(singletonMap("version", Arrays.asList("0", "1")).entrySet());
        }
    }

    @Test
    public void dynamicSink(TestInfo info) throws Exception {
        String tableName = sanitizeDisplay(info);
        tEnv.executeSql(String.join("\n",
            "CREATE TABLE " + tableName + " (",
            "colour VARCHAR,",
            "nats_subject STRING METADATA,",
            "headers MAP<STRING, ARRAY<STRING>> METADATA",
            ") WITH (",
            "'connector' = 'nats',",
            "'format' = 'json',",
            "'io.nats.client.servers' = '" + client().getConnectedUrl() + "'",
            ")"
        ));

        Subscription sub = client().subscribe(tableName + ".*");

        StringBuilder query = new StringBuilder("INSERT INTO " + tableName + " VALUES ");
        for (String colour : colours) {
            query.append(
                String.format("\n('%1$s', '%2$s.%1$s', map['version', array['0', '1']]),", colour, tableName)
            );
        }
        tEnv.executeSql(query.deleteCharAt(query.length() - 1).toString());

        for (String colour: colours) {
            Message msg = sub.nextMessage(1000L);
            client().publish(msg.getReplyTo(), PUBLISH_ACK_BODY);
            Assertions.assertThat(msg.getSubject()).isEqualTo(tableName + "." + colour);
            Assertions.assertThat(new String(msg.getData(), UTF_8)).isEqualTo(String.format(jsonTemplate, colour));
            Assertions.assertThat(msg.getHeaders())
                .isNotNull()
                .extracting(Headers::entrySet)
                .isEqualTo(singletonMap("version", Arrays.asList("0", "1")).entrySet());
        }
    }

    @Test
    public void sourceAndSink(TestInfo info) throws Exception {
        String tableName = sanitizeDisplay(info);
        createStream(tableName, tableName + ".>");
        // Populate stream
        int INIT_MESSAGES = 30;

        for (int i = 0; i < INIT_MESSAGES; i++) {
            client().publish(tableName + ".init." + i,
                new Headers().add("version", "0", "1"),
                String.format(jsonTemplate, colours[i % colours.length]).getBytes(UTF_8)
            );
        }

        tEnv.executeSql(String.join("\n",
            "CREATE TABLE " + tableName + " (",
            "colour VARCHAR,",
            "`timestamp` TIMESTAMP_LTZ(9) METADATA VIRTUAL,",
            "nats_subject STRING METADATA,",
            "headers MAP<STRING, ARRAY<STRING>> METADATA",
            ") WITH (",
            "'connector' = 'nats',",
            "'format' = 'json',",
            "'stream' = '" + tableName + "',",
            "'io.nats.client.servers' = '" + client().getConnectedUrl() + "'",
            ")"
        ));

        Subscription sub = client().subscribe(tableName + ".*");

        tEnv.executeSql(String.join("\n",
            "INSERT INTO " + tableName,
            "SELECT colour, REGEXP_REPLACE(nats_subject, 'init.*', colour), headers",
            "FROM " + tableName,
            "LIMIT " + INIT_MESSAGES
        ));

        Map<String, String> expected = new HashMap<>();
        for (String colour : colours) {
            expected.put( tableName + "." + colour, String.format(jsonTemplate, colour));
        }
        int i = 0;
        while (i < INIT_MESSAGES) {
            Message msg = sub.nextMessage(1000L);
            client().publish(msg.getReplyTo(), PUBLISH_ACK_BODY);
            i++;
            String expectedMsg = expected.get(msg.getSubject());
            Assertions.assertThat(expectedMsg).isNotNull();
            Assertions.assertThat(new String(msg.getData(), UTF_8)).isEqualTo(expectedMsg);
            Assertions.assertThat(msg.getHeaders())
                .isNotNull()
                .extracting(Headers::entrySet)
                .isEqualTo(singletonMap("version", Arrays.asList("0", "1")).entrySet());
        }

        long natsMessages = client()
            .jetStreamManagement()
            .getStreamInfo(tableName)
            .getStreamState()
            .getLastSequence();

        deleteStream(tableName);
        Assertions.assertThat(natsMessages).isEqualTo(INIT_MESSAGES * 2);
    }
}
