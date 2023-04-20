package com.github.chrisss93.connector.nats.source;

import com.github.chrisss93.connector.nats.testutils.NATSTestContext;
import com.github.chrisss93.connector.nats.testutils.NatsTestContextFactory;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.SinkCollector;
import com.github.chrisss93.connector.nats.testutils.source.JetStreamSourceContext;
import com.github.chrisss93.connector.nats.testutils.source.cases.AckEachContext;
import com.github.chrisss93.connector.nats.testutils.source.cases.MultiThreadedFetcherContext;
import com.github.chrisss93.connector.nats.testutils.source.cases.SingleStreamContext;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfoOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.connector.testframe.utils.CollectIteratorAssertions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class JetStreamSourceITCase extends SourceTestSuiteBase<String> {

    // Defines test environment on Flink MiniCluster
    @SuppressWarnings("unused")
    @TestEnv
    MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines external nats-server system
    @TestExternalSystem
    NatsTestEnvironment natsEnv = new NatsTestEnvironment();

    // Checkpointing semantics in source connector are not configurable.
    @SuppressWarnings("unused")
    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[]{EXACTLY_ONCE};

    // Defines an external context Factories,
    // so test cases will be invoked using these external contexts.
    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSourceContext> singleStream =
        new NatsTestContextFactory<>(natsEnv, SingleStreamContext::new);


    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSourceContext> ackEach =
        new NatsTestContextFactory<>(natsEnv, AckEachContext::new);

    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSourceContext> multiThreadedFetcher =
        new NatsTestContextFactory<>(natsEnv, (env, testName) -> new MultiThreadedFetcherContext(env, testName, 2));


    @TestTemplate
    @DisplayName("Dynamic split discovery")
    public void discoverAddRemoveSplits(
        TestEnvironment testEnv,
        DataStreamSourceExternalContext<String> externalContext,
        CheckpointingMode semantic) throws Exception {

        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
            TestingSourceSettings.builder()
                .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                .setCheckpointingMode(semantic)
                .build();
        TestEnvironmentSettings envOptions =
            TestEnvironmentSettings.builder()
                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                .build();
        Source<String, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        int splitNumber = 4;
        List<List<String>> testRecordsLists = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordsLists.add(generateAndWriteTestData(i, externalContext, sourceSettings));
        }

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        execEnv.setRestartStrategy(RestartStrategies.noRestart());
        DataStreamSource<String> stream =
            execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                .setParallelism(splitNumber);
        SinkCollector<String> sinkCollector = SinkCollector.apply(stream);
        JobClient jobClient = submitJob(execEnv, "Source Split Discovery");

        // Step 4: Validate test data
        try (CloseableIterator<String> resultIterator = sinkCollector.build(jobClient)) {
            // Check initial test result
            int testRecordsSize = testRecordsLists.stream().mapToInt(List::size).sum();
            checkResultWithSemantic(resultIterator, testRecordsLists, semantic, testRecordsSize);

            // Step 5: Replace one of the NATS stream's subject-filters created by the generated split data
            StreamConfiguration config = natsEnv.client()
                .jetStreamManagement()
                .getStreamInfo(((NATSTestContext) externalContext).getStreamName(), StreamInfoOptions.allSubjects())
                .getConfiguration();

            List<String> subjects = config.getSubjects();
            Assertions.assertThat(subjects).hasSize(splitNumber);
            String droppedSubject = subjects.remove(0);
            String newSubject = droppedSubject + "-replacement";
            natsEnv.client().jetStreamManagement().updateStream(
                StreamConfiguration.builder(config).addSubjects(newSubject).build()
            );

            // Publish messages to the dropped subject and new subject
            int numNewRecords = 10;
            List<String> newRecords = new ArrayList<>(numNewRecords);
            for (byte i = 0; i < numNewRecords; i++) {
                String message = (newSubject + " - " + i);
                newRecords.add(message);
                natsEnv.client().publish(newSubject, message.getBytes());
                natsEnv.client().publish(droppedSubject, (droppedSubject + " - " + i).getBytes());
            }

            // Expect data for the new subject and none from the dropped subject
            checkResultWithSemantic(resultIterator, singletonList(newRecords),semantic, newRecords.size());
        }
    }


    private void checkResultWithSemantic(
        CloseableIterator<String> resultIterator,
        List<List<String>> testData,
        CheckpointingMode semantic,
        Integer limit) {

        if (limit != null) {
            assertThat(
                CompletableFuture.supplyAsync(
                    () -> {
                        CollectIteratorAssertions.assertThat(resultIterator)
                            .withNumRecordsLimit(limit)
                            .matchesRecordsFromSource(testData, semantic);
                        return true;
                    }))
                .succeedsWithin(DEFAULT_COLLECT_DATA_TIMEOUT);
        } else {
            CollectIteratorAssertions.assertThat(resultIterator)
                .matchesRecordsFromSource(testData, semantic);
        }
    }
}
