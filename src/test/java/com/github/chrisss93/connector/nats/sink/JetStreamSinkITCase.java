package com.github.chrisss93.connector.nats.sink;

import com.github.chrisss93.connector.nats.testutils.NatsTestContextFactory;
import com.github.chrisss93.connector.nats.testutils.NatsTestEnvironment;
import com.github.chrisss93.connector.nats.testutils.runtime.JVMRuntime;
import com.github.chrisss93.connector.nats.testutils.sink.JetStreamSinkContext;
import com.github.chrisss93.connector.nats.testutils.sink.cases.MultiSubjectContext;
import com.github.chrisss93.connector.nats.testutils.sink.cases.SingleSubjectContext;
import com.github.chrisss93.connector.nats.testutils.sink.cases.SmallBufferContext;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkExternalContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.junit.jupiter.api.Disabled;

import static org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE;

public class JetStreamSinkITCase extends SinkTestSuiteBase<String> {

    // Defines test environment on Flink MiniCluster
    @SuppressWarnings("unused")
    @TestEnv
    MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines external nats-server system
    @TestExternalSystem
    NatsTestEnvironment natsEnv = new NatsTestEnvironment(new JVMRuntime());


    // Checkpointing semantics in sink connector are not configurable.
    @SuppressWarnings("unused")
    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {AT_LEAST_ONCE};

    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSinkContext> singleSubject =
        new NatsTestContextFactory<>(natsEnv, SingleSubjectContext::new);

    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSinkContext> multiSubject =
        new NatsTestContextFactory<>(natsEnv, (env, testName) -> new MultiSubjectContext(env, testName, 10));

    @SuppressWarnings("unused")
    @TestContext
    NatsTestContextFactory<JetStreamSinkContext> smallBuffer =
        new NatsTestContextFactory<>(natsEnv, (env, testName) -> new SmallBufferContext(env, testName, 1));
}
