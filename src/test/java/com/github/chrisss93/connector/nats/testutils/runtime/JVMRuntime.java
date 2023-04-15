package com.github.chrisss93.connector.nats.testutils.runtime;

import nats.io.LoggingOutput;
import nats.io.NatsServerRunner;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.logging.Logger;

/** This runtime is more convenient for local development than {@link ContainerRuntime} as we avoid the overhead
 * of starting containers. This runtime requires the nats-server binary to be on the user's $PATH.
 */
public class JVMRuntime implements NatsServerRuntime {

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        Logger LOG = Logger.getLogger(JVMRuntime.class.getName());
        NatsServerRunner.setDefaultOutputSupplier(() -> new LoggingOutput(LOG));
    }
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
