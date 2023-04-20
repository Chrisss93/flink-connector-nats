package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.testutils.NatsTestSuiteBase;
import io.nats.client.impl.Headers;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.github.chrisss93.connector.nats.table.JetStreamConnectorOptions.StopRuleEnum.NeverStop;

public class JetStreamDynamicTableSourceITCase extends NatsTestSuiteBase {
    private static final int NUM_RECORDS = 3;
    private static final int CURRENT_YEAR = 2023;
    private static final String msgTemplate = "{\"id\": %d, \"name\": \"%s\", \"age\": %d}";
    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tEnv;


    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).build()
    );

    @BeforeEach
    public void beforeEach(TestInfo info) throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);
        String streamName = sanitizeDisplay(info);
        createStream(streamName, streamName + ".>");
        // Populate stream
        for (int i = 0; i < NUM_RECORDS; i++) {
            client().publish(streamName + ".standard." + i,
                new Headers().add("one", "foo", "bar").add("two", "blue"),
                String.format(msgTemplate, i, alphabet(i), i * 10).getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    @AfterEach
    public void afterEach(TestInfo info) throws Exception {
        deleteStream(sanitizeDisplay(info));
    }

    @Test
    public void selectStar(TestInfo info) throws Exception {
        String streamName = sanitizeDisplay(info);
        tEnv.executeSql(createTableDDL(streamName));

        List<Row> collectedRows = new ArrayList<>();
        try(CloseableIterator<Row> collected = tEnv.executeSql("SELECT * FROM nats_source").collect()) {
            while (collected.hasNext()) {
                collectedRows.add(collected.next());
            }
        }

        List<Row> expected = expectedRows(streamName);
        Assertions.assertThat(collectedRows).isEqualTo(expected);
    }

    @Test
    public void limitPushDown(TestInfo info) throws Exception {
        String streamName = sanitizeDisplay(info);
        tEnv.executeSql(createTableDDL(streamName, Collections.singletonMap("stop.rule", NeverStop.name())));

        // Publish extra malformed message to NATS stream
        client().publish(streamName + ".bad", new byte[]{1});

        List<Row> collectedRows = new ArrayList<>();
        try(CloseableIterator<Row> collected = tEnv.executeSql(
            "SELECT * FROM nats_source LIMIT " + NUM_RECORDS).collect()) {
            while (collected.hasNext()) {
                collectedRows.add(collected.next());
            }
        }
        // Since limit clause is pushed down, the malformed record is never collected in the first place
        List<Row> expected = expectedRows(streamName);
        Assertions.assertThat(collectedRows).isEqualTo(expected);
    }

    @Test
    public void filterPushDownEqual(TestInfo info) throws Exception {
        String streamName = sanitizeDisplay(info);
        String[] subjects = new String[NUM_RECORDS];
        for (int i = 0; i < NUM_RECORDS; i++) {
            subjects[i] = String.format("subject = '%s.standard.%d'", streamName, i);
        }

        String predicate = String.join(" OR ", subjects);
        filterPushDownQuery(streamName, predicate, false);
    }

    @Test
    public void filterPushDownIn(TestInfo info) throws Exception {
        String streamName = sanitizeDisplay(info);
        String[] subjects = new String[NUM_RECORDS];
        for (int i = 0; i < NUM_RECORDS; i++) {
            subjects[i] = String.format("'%s.standard.%d'", streamName, i);
        }

        String predicate = "subject IN (" + String.join(", ", subjects) + ")";
        filterPushDownQuery(streamName, predicate, false);
    }

    @Test
    public void filterPushDownLike(TestInfo info) throws Exception {
        String streamName = sanitizeDisplay(info);
        String predicate = String.format("subject LIKE '%s.standard.%%'", streamName);
        filterPushDownQuery(streamName, predicate, true);
    }

    // TODO: How to easily test watermark-push-down ability?

    private String createTableDDL(String stream) {
        return createTableDDL(stream, new HashMap<>());
    }
    private String createTableDDL(String stream, Map<String, String> extraOptions) {
        return String.join("\n", Arrays.asList(
            "CREATE TABLE nats_source (",
            "subject VARCHAR METADATA,",
            "id BIGINT,",
            "name VARCHAR,",
            "age INT,",
            "DOB AS " + CURRENT_YEAR + " - age,",
            "headers MAP<VARCHAR, ARRAY<VARCHAR>> METADATA",
            ") WITH (",
            "'connector' = 'nats',",
            "'format' = 'json',",
            "'consumer.prefix' = 'test',",
            "'consumer.extra.props' = 'name:test,ack_wait:10e9',",
            "'stream' = '" + stream + "',",
            "'stop.rule' = 'NumMessageStop',",
            "'stop.value' = '" + NUM_RECORDS + "',",
            String.format("'io.nats.client.servers' = '%s'", client().getConnectedUrl()),
            extraOptions.entrySet().stream()
                .map(e -> String.format(",'%s' = '%s'", e.getKey(), e.getValue()))
                .reduce((l, r) -> l + "\n" + r)
                .orElse(""),
            ")"
        ));
    }

    private void filterPushDownQuery(String stream, String predicate, boolean ordered) throws Exception {
//        Map<String, String> extraProps = new HashMap<>();
//        extraProps.put("stop.rule", TimerStop.name());
//        extraProps.put("stop.value", String.valueOf(10000L));

        tEnv.executeSql(createTableDDL(stream, Collections.singletonMap("stop.rule", NeverStop.name())));

        // Publish extra malformed message to NATS stream
        client().publish(stream + ".bad", new byte[]{1});

        String query = "SELECT * FROM nats_source WHERE " + predicate + "LIMIT 4";
        List<Row> collectedRows = new ArrayList<>();
        try(CloseableIterator<Row> collected = tEnv.executeSql(query).collect()) {
            while (collected.hasNext()) {
                collectedRows.add(collected.next());
            }
        }
        List<Row> expected = expectedRows(stream);
        // A bit of a hack here. Since checkpointing is disabled, source will not ack messages and NATS will
        // eventually send the messages again. We rely on the message retry to trigger the stopping condition. But
        // this also means the first message retry will be included in the result, so we drop it.
        Assertions.assertThat(collectedRows).hasSize(4);
        ListAssert<Row> a = Assertions.assertThat(collectedRows.subList(0, 3));
        if (ordered) {
            a.isEqualTo(expected);
        } else {
            a.containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private static final LinkedHashMap<String, Integer> FIELD_POSITIONS;
    static {
        FIELD_POSITIONS = new LinkedHashMap<>();
        FIELD_POSITIONS.put("subject", 0);
        FIELD_POSITIONS.put("id", 1);
        FIELD_POSITIONS.put("name", 2);
        FIELD_POSITIONS.put("age", 3);
        FIELD_POSITIONS.put("DOB", 4);
        FIELD_POSITIONS.put("streamSeq", 5);
    }

    private List<Row> expectedRows(String streamName) {
        Map<String, String[]> headers = new HashMap<>();
        headers.put("one", new String[]{"foo", "bar"});
        headers.put("two", new String[]{"blue"});

        ArrayList<Row> expected = new ArrayList<>(NUM_RECORDS);
        for (int i = 0; i < NUM_RECORDS; i++) {
            Row row = RowUtils.createRowWithNamedPositions(
                RowKind.INSERT,
                new Object[]{
                    streamName + ".standard." + i,
                    (long) i,
                    String.valueOf(alphabet(i)),
                    i * 10,
                    CURRENT_YEAR - i * 10,
                    headers
                },
                FIELD_POSITIONS
            );
            expected.add(row);
        }
        return expected;
    }

    private char alphabet(int i) {
        String letters = "abcdefghijklmnopqrstuvwxyz";
        if (i < letters.length()) {
            return letters.charAt(i);
        } else {
            return letters.charAt(i % letters.length());
        }
    }
}
