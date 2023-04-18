package com.github.chrisss93.connector.nats.testutils;

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfoOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.testframe.external.ExternalContext;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.Collections;
import java.util.List;

public abstract class NATSTestContext implements ExternalContext, ResultTypeQueryable<String>, Serializable {

    protected final transient NatsTestEnvironment runtime;
    protected final String streamName;

    public NATSTestContext(NatsTestEnvironment runtime, String streamName) {
        this.runtime = runtime;
        this.streamName = streamName;
    }

    protected final void makeStream(String subjectFilter) {
        try {
            runtime.client().jetStreamManagement().addStream(
                new StreamConfiguration.Builder()
                    .name(streamName)
                    .subjects(subjectFilter)
                    .build()
            );
        } catch (IOException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    protected final void updateStream(String extraSubjectFilter) {
        try {
            JetStreamManagement jsm = runtime.client().jetStreamManagement();
            StreamConfiguration config = jsm.getStreamInfo(streamName, StreamInfoOptions.allSubjects())
                .getConfiguration();
            jsm.updateStream(StreamConfiguration.builder(config).addSubjects(extraSubjectFilter).build());
        } catch (JetStreamApiException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getStreamName() {
        return streamName;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
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
