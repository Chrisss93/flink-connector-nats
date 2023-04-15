package com.github.chrisss93.connector.nats.testutils.runtime;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

public class ContainerRuntime implements NatsServerRuntime {

    private static final Slf4jLogConsumer logger = new Slf4jLogConsumer(
        LoggerFactory.getLogger(ContainerRuntime.class)
    );

    private static final String IMAGE_NAME = "nats";
    private static final String IMAGE_TAG = "2.9";
    private static final int NATS_PORT = 4222;

    @SuppressWarnings("resource")
    private final GenericContainer<?> natsContainer = new GenericContainer<>(IMAGE_NAME + ":" + IMAGE_TAG)
        .withCommand("-js")
        .withExposedPorts(NATS_PORT)
        .withLogConsumer(logger)
        .waitingFor(Wait.forLogMessage(".*Server is ready.*", 1));

    @Override
    public void startUp() {
        natsContainer.start();
    }

    @Override
    public void tearDown() {
        natsContainer.close();
    }

    @Override
    public String address() {
        return natsContainer.getHost() + ":" + natsContainer.getMappedPort(NATS_PORT);
    }
}
