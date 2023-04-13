package com.github.chrisss93.connector.nats.testutils;

import org.apache.flink.connector.testframe.external.ExternalContext;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;

import java.util.function.BiFunction;
import java.util.function.Function;

public class NatsTestContextFactory<T extends ExternalContext> implements ExternalContextFactory<T> {

    private final NatsTestEnvironment env;
    private final BiFunction<NatsTestEnvironment, String, T> contextFactory;

    public NatsTestContextFactory(NatsTestEnvironment env, BiFunction<NatsTestEnvironment, String, T> contextFactory) {
        this.env = env;
        this.contextFactory = contextFactory;
    }
    @Override
    public T createExternalContext(String testName) {
        return contextFactory.apply(env, testName.replaceAll("[^A-Za-z0-9]",""));
    }
}
