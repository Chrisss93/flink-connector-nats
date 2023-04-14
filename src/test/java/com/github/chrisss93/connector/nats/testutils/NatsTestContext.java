package com.github.chrisss93.connector.nats.testutils;

import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamConfiguration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.testframe.external.ExternalContext;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class NatsTestContext implements ExternalContext, ResultTypeQueryable<String>, Serializable {

    protected final transient NatsTestEnvironment runtime;
    protected final String streamName;

    public NatsTestContext(NatsTestEnvironment runtime, String streamName) {
        this.runtime = runtime;
        this.streamName = streamName;
    }

    protected final void makeStream() {
        try {
            runtime.client().jetStreamManagement().addStream(
                new StreamConfiguration.Builder()
                    .name(streamName)
                    .subjects(streamSubjectFilters())
                    .build()
            );
        } catch (IOException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract Collection<String> streamSubjectFilters();
    protected abstract String testDataToSubject(String testData);

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        runtime.client().jetStreamManagement().deleteStream(streamName);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

}
