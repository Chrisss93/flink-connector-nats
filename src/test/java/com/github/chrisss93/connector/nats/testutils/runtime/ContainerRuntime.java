package com.github.chrisss93.connector.nats.testutils.runtime;

// TODO: Implement testcontainers version of the NatsServerRuntime
public class ContainerRuntime implements NatsServerRuntime {
    @Override
    public void startUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {

    }

    @Override
    public String address() {
        return null;
    }
}
