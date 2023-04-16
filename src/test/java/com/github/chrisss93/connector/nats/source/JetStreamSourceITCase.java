package com.github.chrisss93.connector.nats.source;

import com.github.chrisss93.connector.nats.testutils.NatsTestContextFactory;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.source.*;
import com.github.chrisss93.connector.nats.testutils.source.cases.MultiFilterStreamContext;
import com.github.chrisss93.connector.nats.testutils.source.cases.MultiThreadedFetcherContext;
import com.github.chrisss93.connector.nats.testutils.source.cases.SingleFilterStreamContext;
import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.connector.testframe.utils.MetricQuerier;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.function.SupplierWithException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.connector.testframe.utils.MetricQuerier.getJobDetails;
import static org.apache.flink.runtime.testutils.CommonTestUtils.terminateJob;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForJobStatus;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;

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
    NatsTestContextFactory<JetStreamSourceContext> singleFilterStream =
        new NatsTestContextFactory<>(natsEnv, SingleFilterStreamContext::new);

    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSourceContext> multiFilterStream =
        new NatsTestContextFactory<>(natsEnv, (env, testName) -> new MultiFilterStreamContext(env, testName, 4));

    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSourceContext> multiThreadedFetcher =
        new NatsTestContextFactory<>(natsEnv, (env, testName) -> new MultiThreadedFetcherContext(env, testName, 2));

    /**
        Same as the existing implementation except that job metrics will be queried once all job tasks are
        running OR finished. This is needed here because not all generated split data by the test-contexts will be
        interpreted as unique splits by the source reader, so jobs may correctly have fewer running tasks than the
        original test expects due to idle parallelism.
     **/
    @Override
    @TestTemplate
    @DisplayName("Test source metrics")
    public void testSourceMetrics(
        TestEnvironment testEnv,
        DataStreamSourceExternalContext<String> externalContext,
        CheckpointingMode semantic) throws Exception {

        TestingSourceSettings sourceSettings = TestingSourceSettings.builder()
                .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                .setCheckpointingMode(semantic)
                .build();
        TestEnvironmentSettings envOptions = TestEnvironmentSettings.builder()
                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                .build();

        final int splitNumber = 4;
        final List<List<String>> testRecordCollections = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordCollections.add(generateAndWriteTestData(i, externalContext, sourceSettings));
        }

        // make sure use different names when executes multi times
        String sourceName = "metricTestSource" + testRecordCollections.hashCode();
        final StreamExecutionEnvironment env = testEnv.createExecutionEnvironment(envOptions);
        env
            .fromSource(
                tryCreateSource(externalContext, sourceSettings),
                WatermarkStrategy.noWatermarks(),
                sourceName)
            .setParallelism(splitNumber)
            .addSink(new DiscardingSink<>());
        final JobClient jobClient = env.executeAsync("Metrics Test");

        final MetricQuerier queryRestClient = new MetricQuerier(new Configuration());
        final ExecutorService executorService = Executors.newCachedThreadPool();

        try {
            waitForAllTaskState( // instead of waitForAllTaskRunning
                Arrays.asList(ExecutionState.RUNNING, ExecutionState.FINISHED),
                () -> getJobDetails(
                        new RestClient(new Configuration(), executorService),
                        testEnv.getRestEndpoint(),
                        jobClient.getJobID()));

            waitUntilCondition( // Compare metrics
                () -> {
                    try {
                        Double sumNumRecordsIn = queryRestClient.getAggregatedMetricsByRestAPI(
                            testEnv.getRestEndpoint(),
                            jobClient.getJobID(),
                            sourceName,
                            MetricNames.IO_NUM_RECORDS_IN,
                            null);
                        return Precision.equals(getTestDataSize(testRecordCollections), sumNumRecordsIn);
                    } catch(Exception e) {
                        return false;
                    }
                });
        } finally {
            // Clean up
            executorService.shutdown();
            killJob(jobClient);
        }
    }

    private static void waitForAllTaskState(
        List<ExecutionState> goodStates,
        SupplierWithException<JobDetailsInfo, Exception> jobDetailsSupplier) throws Exception {

        waitUntilCondition(
            () -> {
                final JobDetailsInfo jobDetailsInfo = jobDetailsSupplier.get();
                final Collection<JobDetailsInfo.JobVertexDetailsInfo> vertexInfos =
                    jobDetailsInfo.getJobVertexInfos();
                if (vertexInfos.size() == 0) {
                    return false;
                }
                for (JobDetailsInfo.JobVertexDetailsInfo vertexInfo : vertexInfos) {
                    final int numGoodTasks =
                        vertexInfo.getTasksPerState().entrySet()
                            .stream()
                            .filter(e -> goodStates.contains(e.getKey()))
                            .mapToInt(Map.Entry::getValue)
                            .sum();

                    if (numGoodTasks != vertexInfo.getParallelism()) {
                        return false;
                    }
                }
                return true;
            }
        );
    }

    private void killJob(JobClient jobClient) throws Exception {
        terminateJob(jobClient);
        waitForJobStatus(jobClient, Collections.singletonList(JobStatus.CANCELED));
    }
}
