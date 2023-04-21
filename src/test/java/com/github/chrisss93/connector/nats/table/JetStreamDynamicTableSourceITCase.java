package com.github.chrisss93.connector.nats.table;

import com.github.chrisss93.connector.nats.testutils.NatsTestSuiteBase;
import com.github.chrisss93.connector.nats.testutils.SinkCollector;
import io.nats.client.impl.Headers;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.messages.*;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.github.chrisss93.connector.nats.table.JetStreamConnectorOptions.StopRuleEnum.NumMessageStop;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class JetStreamDynamicTableSourceITCase extends NatsTestSuiteBase {
    private static final int NUM_SUBJECTS = 10;
    private static final int CURRENT_YEAR = 2023;
    private static final String jsonTemplate = "{\"id\": %d, \"name\": \"%s\", \"age\": %d}";
    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tEnv;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension();

    @BeforeEach
    public void beforeEach(TestInfo info) throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        String streamName = sanitizeDisplay(info);
        createStream(streamName, streamName + ".>");
        // Populate stream
        for (int i = 0; i < NUM_SUBJECTS; i++) {
            client().publish(streamName + ".standard." + i,
                new Headers().add("one", "foo", "bar").add("two", "blue"),
                String.format(jsonTemplate, i, alphabet(i), i * 10).getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    @AfterEach
    public void afterEach(TestInfo info) throws Exception {
        deleteStream(sanitizeDisplay(info));
    }

    @Test
    public void selectStar(TestInfo info) throws Exception {
        Map<String, String> extraProps = new HashMap<>();
        extraProps.put("stop.rule", NumMessageStop.name());
        extraProps.put("stop.value", String.valueOf(NUM_SUBJECTS));
        String streamName = sanitizeDisplay(info);
        tEnv.executeSql(createTableDDL(streamName, extraProps));

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
        tEnv.executeSql(createTableDDL(streamName));

        // Publish extra malformed message to NATS stream
        client().publish(streamName + ".bad", new byte[]{1});

        List<Row> collectedRows = new ArrayList<>();
        try(CloseableIterator<Row> collected = tEnv.executeSql(
            "SELECT * FROM nats_source LIMIT " + NUM_SUBJECTS).collect()) {
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
        String[] subjects = new String[NUM_SUBJECTS];
        for (int i = 0; i < NUM_SUBJECTS; i++) {
            subjects[i] = String.format("nats_subject = '%s.standard.%d'", streamName, i);
        }

        String predicate = String.join(" OR ", subjects);
        filterPushDownQuery(streamName, predicate, false);
    }

    @Test
    public void filterPushDownIn(TestInfo info) throws Exception {
        String streamName = sanitizeDisplay(info);
        String[] subjects = new String[NUM_SUBJECTS];
        for (int i = 0; i < NUM_SUBJECTS; i++) {
            subjects[i] = String.format("'%s.standard.%d'", streamName, i);
        }

        String predicate = "nats_subject IN (" + String.join(", ", subjects) + ")";
        filterPushDownQuery(streamName, predicate, false);
    }

    @Test
    public void filterPushDownLike(TestInfo info) throws Exception {
        String streamName = sanitizeDisplay(info);
        String predicate = String.format("nats_subject LIKE '%s.standard.%%'", streamName);
        filterPushDownQuery(streamName, predicate, true);
    }


    // This test needs to convert table query back to datastream API so the job can run asynchronously.
    // This is required to check the exposed job metrics before the MiniCluster takes itself down.
    @Test
    public void filterPushDownPartial(TestInfo info,
                                      @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {

        Map<String, String> extraProps = new HashMap<>();
        String streamName = sanitizeDisplay(info);
//        env.setParallelism(1);
        tEnv.executeSql(createTableDDL(streamName, extraProps));

        // Publish extra malformed message to NATS stream
        client().publish(streamName + ".bad", new byte[]{1});

        DataStream<Row> datastream = tEnv.toDataStream(
            tEnv.sqlQuery(
                "SELECT * FROM nats_source WHERE id % 2 = 0 AND nats_subject LIKE '" + streamName + ".standard.%'"
            )
        );
        SinkCollector<Row> sinkCollector = SinkCollector.apply(datastream);
        JobClient jobClient = env.executeAsync(streamName + " job");

        Iterator<Row> expectedIter = expectedRows(streamName).stream()
            .filter(r -> (long) (r.getField("id")) % 2 == 0)
            .iterator();

        try (CloseableIterator<Row> iterator = sinkCollector.build(jobClient)) {
            int i = 0;
            Row firstMessage = null;
            // Checking results
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (firstMessage == null) {
                    firstMessage = row;
                }

                Row expected;
                if (expectedIter.hasNext()) {
                    expected = expectedIter.next();
                } else {
                    expected = firstMessage;
                }
                assertThat(row).isEqualTo(expected);
                if (i++ == NUM_SUBJECTS / 2) {
                    break;
                }
            }
            // Checking metrics
            Optional<Double> recordsWritten = recordsWrittenMetric(restClusterClient, jobClient.getJobID());
            Assertions.assertThat(recordsWritten).isEqualTo(Optional.of(NUM_SUBJECTS * 2d));

        }
    }

    // TODO: How to define test for watermark-push-down ability?

    private String createTableDDL(String stream) {
        return createTableDDL(stream, new HashMap<>());
    }
    private String createTableDDL(String stream, Map<String, String> extraOptions) {
        return String.join("\n", Arrays.asList(
            "CREATE TABLE nats_source (",
            "nats_subject VARCHAR METADATA,",
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
            String.format("'io.nats.client.servers' = '%s'", client().getConnectedUrl()),
            extraOptions.entrySet().stream()
                .map(e -> String.format(",'%s' = '%s'", e.getKey(), e.getValue()))
                .reduce((l, r) -> l + "\n" + r)
                .orElse(""),
            ")"
        ));
    }

    private void filterPushDownQuery(String stream, String predicate, boolean ordered) throws Exception {
        tEnv.executeSql(createTableDDL(stream));
        // Publish extra malformed message to NATS stream
        client().publish(stream + ".bad", new byte[]{1});

        String query = "SELECT * FROM nats_source WHERE " + predicate + " LIMIT " + (NUM_SUBJECTS + 1);
        System.out.println(query);
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
        Assertions.assertThat(collectedRows).hasSize(NUM_SUBJECTS + 1);
        ListAssert<Row> a = Assertions.assertThat(collectedRows.subList(0, collectedRows.size() - 1));
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
        FIELD_POSITIONS.put("headers", 5);
    }

    private List<Row> expectedRows(String streamName) {
        Map<String, String[]> headers = new HashMap<>();
        headers.put("one", new String[]{"foo", "bar"});
        headers.put("two", new String[]{"blue"});

        ArrayList<Row> expected = new ArrayList<>(NUM_SUBJECTS);
        for (int i = 0; i < NUM_SUBJECTS; i++) {
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

    private static Optional<Double> recordsWrittenMetric(RestClusterClient<?> restClusterClient, JobID jobId) throws Exception {
        JobDetailsInfo jobDetails = restClusterClient.getJobDetails(jobId).get();
        JobVertexID vertexId = jobDetails
            .getJobVertexInfos().stream()
            .filter(v -> v.getName().startsWith("Source: "))
            .findFirst()
            .map(JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID)
            .orElse(null);

        Assertions.assertThat(vertexId).isNotNull();

        Optional<String> metricList = getMetrics(restClusterClient, jobDetails.getJobId(), vertexId, "")
            .getMetrics()
            .stream()
            .map(AggregatedMetric::getId)
            .filter(s -> s.endsWith(MetricNames.IO_NUM_RECORDS_IN) && s.startsWith("Source__"))
            .findFirst();

        Assertions.assertThat(metricList).isPresent();

        return getMetrics(restClusterClient, jobDetails.getJobId(), vertexId, metricList.get())
            .getMetrics().stream()
            .findFirst()
            .map(AggregatedMetric::getSum);
    }

    private static AggregatedMetricsResponseBody getMetrics(RestClusterClient<?> restClusterClient,
                                                            JobID jobId,
                                                            JobVertexID vertexId,
                                                            String filters) throws Exception {

        AggregatedSubtaskMetricsParameters params = new AggregatedSubtaskMetricsParameters();
        Iterator<MessagePathParameter<?>> pathParams = params.getPathParameters().iterator();
        ((JobIDPathParameter) pathParams.next()).resolve(jobId);
        ((JobVertexIdPathParameter) pathParams.next()).resolve(vertexId);
        if (filters.length() > 0) {
            MetricsFilterParameter metricFilter = (MetricsFilterParameter)
                params.getQueryParameters().iterator().next();
            metricFilter.resolveFromString(filters);
        }
        return restClusterClient.sendRequest(
            AggregatedSubtaskMetricsHeaders.getInstance(),
            params,
            EmptyRequestBody.getInstance()
        ).get(10, TimeUnit.SECONDS);
    }
}
