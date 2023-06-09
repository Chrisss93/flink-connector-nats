package com.github.chrisss93.connector.nats.testutils;

import com.github.chrisss93.connector.nats.testutils.runtime.ContainerRuntime;
import com.github.chrisss93.connector.nats.testutils.runtime.JVMRuntime;
import com.github.chrisss93.connector.nats.testutils.runtime.NatsServerRuntime;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.apache.flink.connector.testframe.TestResource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class NatsTestEnvironment implements BeforeAllCallback, AfterAllCallback, TestResource {

    private static final String runnerProp = "nats.test.runtime";
    private static final String jvmRunner = "jvm";
    private static final String containerRunner = "docker";


    private final NatsServerRuntime runtime;
    private Connection client;

    public NatsTestEnvironment() {
        this(
            System.getProperty(runnerProp, containerRunner).equals(jvmRunner)
                ? new JVMRuntime()
                : new ContainerRuntime()
        );
    }

    public NatsTestEnvironment(NatsServerRuntime runtime) {
        this.runtime = runtime;
    }

    public Connection client() {
        return client;
    }


    @Override
    public void startUp() throws Exception {
        runtime.startUp();
        client = Nats.connect(runtime.address());
    }

    @Override
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        runtime.tearDown();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        tearDown();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        startUp();
    }
}
