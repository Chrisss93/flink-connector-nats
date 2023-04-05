package com.github.chrisss93.connector.nats.testutils.runtime;

import nats.io.NatsServerRunner;

/** This runtime is more convenient for local development than {@link ContainerRuntime} as we avoid the overhead
 * of starting containers. This runtime requires the nats-server binary to be on the user's $PATH.
 */
public class JVMRuntime implements NatsServerRuntime {
    private NatsServerRunner server;

    @Override
    public void startUp() throws Exception {
        server = new NatsServerRunner(false, true);
    }

    @Override
    public void tearDown() throws Exception {
        server.close();
    }

    @Override
    public String address() {
        return server.getURI();
    }
}
