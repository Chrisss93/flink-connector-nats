package com.github.chrisss93.connector.nats.source;

import com.github.chrisss93.connector.nats.testutils.NatsTestContextFactory;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.runtime.JVMRuntime;
import com.github.chrisss93.connector.nats.testutils.source.*;
import com.github.chrisss93.connector.nats.testutils.source.cases.MultiFilterStreamContext;
import com.github.chrisss93.connector.nats.testutils.source.cases.MultiThreadedFetcherContext;
import com.github.chrisss93.connector.nats.testutils.source.cases.SingleFilterStreamContext;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.junit.jupiter.api.Disabled;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;

public class JetStreamSourceITCase extends SourceTestSuiteBase<String> {

    // Defines test environment on Flink MiniCluster
    @SuppressWarnings("unused")
    @TestEnv
    MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines external nats-server system
    @TestExternalSystem
    NatsTestEnvironment natsEnv = new NatsTestEnvironment(new JVMRuntime());

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

    @Disabled("enable once telemetry is implemented")
    @Override
    public void testSourceMetrics(
        TestEnvironment testEnv,
        DataStreamSourceExternalContext<String> externalContext,
        CheckpointingMode semantic) {
    }
}
