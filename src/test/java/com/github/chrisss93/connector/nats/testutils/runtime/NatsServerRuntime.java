package com.github.chrisss93.connector.nats.testutils.runtime;

public interface NatsServerRuntime {
    void startUp() throws Exception;

    void tearDown() throws Exception;

    String address();
}
