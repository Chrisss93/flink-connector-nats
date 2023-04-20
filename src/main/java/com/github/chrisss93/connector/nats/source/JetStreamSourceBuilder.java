package com.github.chrisss93.connector.nats.source;

import com.github.chrisss93.connector.nats.common.SubjectUtils;
import com.github.chrisss93.connector.nats.source.reader.deserializer.NATSMessageDeserializationSchema;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.NeverStop;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.StartRule;
import com.github.chrisss93.connector.nats.source.enumerator.offsets.StopRule;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import static io.nats.client.support.Validator.validateStreamName;
import static org.apache.flink.util.Preconditions.*;

public class JetStreamSourceBuilder<T> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceBuilder.class);

    private String stream;
    private final Properties connectProps = new Properties();
    private StopRule stopRule = new NeverStop();
    private StartRule startRule = StartRule.EARLIEST;
    private long startValue = -1;
    private NATSMessageDeserializationSchema<T> deserializationSchema;
    private final Set<NATSConsumerConfig> consumers = new HashSet<>();
    private NATSConsumerConfig defaultConsumer;
    private boolean discoverSplits = false;
    private boolean ackMessageOnCheckpoint = true;
    private boolean ackEachMessage = false;
    private int numFetchersPerReader = 1;
    private long splitDiscoveryIntervalMs = 300000L;

    public JetStreamSourceBuilder<T> setServerURL(String address) {
        connectProps.setProperty(Options.PROP_URL, address);
        return this;
    }

    public JetStreamSourceBuilder<T> setServerURLs(String[] addresses) {
        connectProps.setProperty(Options.PROP_SERVERS, String.join(",", addresses));
        return this;
    }

    public JetStreamSourceBuilder<T>  setConnectionUserInfo(String username, String password) {
        connectProps.setProperty(Options.PROP_USERNAME, username);
        connectProps.setProperty(Options.PROP_PASSWORD, password);
        return this;
    }

    public JetStreamSourceBuilder<T>  setConnectionToken(String token) {
        connectProps.setProperty(Options.PROP_TOKEN, token);
        return this;
    }

    public JetStreamSourceBuilder<T> setConnectionProperties(Map<?, ?> props) {
        connectProps.putAll(props);
        return this;
    }

    public JetStreamSourceBuilder<T> setDeserializationSchema(NATSMessageDeserializationSchema<T> schema) {
        this.deserializationSchema = schema;
        return this;
    }

    public JetStreamSourceBuilder<T> setStoppingRule(StopRule stopRule) {
        this.stopRule = stopRule;
        return this;
    }

    public JetStreamSourceBuilder<T> setStartingRule(StartRule startRule) {
        this.startRule = startRule;
        return this;
    }

    public JetStreamSourceBuilder<T> setStartingValue(long value) {
        this.startValue = value;
        return this;
    }

    public JetStreamSourceBuilder<T> setStream(String stream) {
        validateStreamName(stream, true);
        this.stream = stream;
        return this;
    }

    public JetStreamSourceBuilder<T> addConsumerConfiguration(ConsumerConfiguration conf) {
        return addConsumerConfiguration(new ConsumerConfiguration.Builder(conf));
    }

    public JetStreamSourceBuilder<T> addConsumerConfiguration(ConsumerConfiguration.Builder builder) {
        consumers.add(new NATSConsumerConfig(builder.build()));
        return this;
    }
    public void clearConsumerConfigurations() {
        consumers.clear();
    }

    public JetStreamSourceBuilder<T> setDefaultConsumerConfiguration(String prefix) {
        return setDefaultConsumerConfiguration(new ConsumerConfiguration.Builder().name(prefix));
    }

    public JetStreamSourceBuilder<T> setDefaultConsumerConfiguration(ConsumerConfiguration conf) {
        return setDefaultConsumerConfiguration(new ConsumerConfiguration.Builder(conf));
    }

    public JetStreamSourceBuilder<T> setDefaultConsumerConfiguration(ConsumerConfiguration.Builder builder) {
        this.discoverSplits = true;
        defaultConsumer = new NATSConsumerConfig(builder.build());
        return this;
    }
    public void clearDefaultConsumerConfiguration() {
        this.defaultConsumer = null;
        this.discoverSplits = false;
    }

    public JetStreamSourceBuilder<T> ackMessagesOnCheckpoint(boolean b) {
        ackMessageOnCheckpoint = b;
        return this;
    }

    public JetStreamSourceBuilder<T> ackEachMessage(boolean b) {
        this.ackEachMessage = b;
        return this;
    }

    public JetStreamSourceBuilder<T> setNumFetchersPerReader(int numFetchers) {
        this.numFetchersPerReader = numFetchers;
        return this;
    }

    public JetStreamSourceBuilder<T> setSplitDiscoveryInterval(long milliseconds) {
        this.splitDiscoveryIntervalMs = milliseconds;
        return this;
    }


    public JetStreamSource<T> build() {
        if ((startRule == StartRule.FROM_STREAM_SEQUENCE || startRule == StartRule.FROM_TIME) && startValue < 0) {
            throw new IllegalStateException(
                "If startRule is set to FROM_STREAM_SEQUENCE or FROM_TIME, a non-negative startValue must be set " +
                    "to indicate the stream-sequence number or message timestamp in epoch milliseconds to start from."
            );
        }

        if (consumers.size() == 0 && defaultConsumer == null) {
            throw new IllegalStateException("Must add at least one consumerConfiguration or set a " +
                "defaultConsumerConfiguration");
        } else if (consumers.size() > 0 && defaultConsumer != null) {
            throw new IllegalStateException("Cannot specify both a consumerConfiguration and a " +
                "defaultConsumerConfiguration");
        }

        checkArgument(deserializationSchema != null, "deserializationSchema must be set.");
        checkArgument(stream != null, "stream must be set.");
        checkArgument(numFetchersPerReader > 0, "numFetchersPerReader must be set to a positive value");
        validateStreamName(stream, true);
        new Options.Builder(connectProps).build();
        if (!discoverSplits && splitDiscoveryIntervalMs > 0) {
            LOG.warn("splitFilterDiscoveryInterval is set but defaultConsumerConfiguration is not. The connector" +
                " will NOT periodically look for any changes in the stream's subject-filters to add/revoke splits");
        }

        return new JetStreamSource<>(
            connectProps,
            deserializationSchema,
            stream,
            makeNATSConsumerConfig(discoverSplits ? Collections.singleton(defaultConsumer) : consumers),
            stopRule,
            discoverSplits,
            splitDiscoveryIntervalMs,
            ackMessageOnCheckpoint,
            ackEachMessage,
            numFetchersPerReader
        );
    }

    public Connection getConnection() throws IOException, InterruptedException {
        return Nats.connect(new Options.Builder(connectProps).build());
    }

    public String getStream() {
        return stream;
    }

    public NATSConsumerConfig getDefaultConsumer() {
        return defaultConsumer;
    }

    private Set<NATSConsumerConfig>  makeNATSConsumerConfig(
        Collection<? extends ConsumerConfiguration.Builder> configs) {

        Set<NATSConsumerConfig> natsConfig = new HashSet<>(configs.size());
        List<String> filters = new ArrayList<>(configs.size());

        for (ConsumerConfiguration.Builder builder : configs) {
            switch(startRule) {
                case EARLIEST:
                    builder.deliverPolicy(DeliverPolicy.All);
                    break;
                case LATEST:
                    builder.deliverPolicy(DeliverPolicy.New);
                    break;
                case FROM_TIME:
                    builder.deliverPolicy(DeliverPolicy.ByStartTime);
                    builder.startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(startValue), ZoneId.of("GMT")));
                    break;
                case FROM_STREAM_SEQUENCE:
                    builder.deliverPolicy(DeliverPolicy.ByStartSequence);
                    builder.startSequence(startValue);
                    break;
            }
            if (!ackMessageOnCheckpoint) {
                builder.ackPolicy(AckPolicy.None);
            } else if (ackEachMessage) {
                builder.ackPolicy(AckPolicy.Explicit);
//                builder.maxAckPending(Long.MAX_VALUE - 1);
                builder.maxAckPending(Integer.MAX_VALUE);
            } else {
                builder.ackPolicy(AckPolicy.All);
//                builder.maxAckPending(Long.MAX_VALUE - 1);
                builder.maxAckPending(Integer.MAX_VALUE);
            }

            ConsumerConfiguration config = builder.build();
            filters.add(config.getFilterSubject());
            natsConfig.add(new NATSConsumerConfig(config));
        }

        if (filters.size() > 1) {
            if (SubjectUtils.overlappingFilterSubjects(filters.toArray(new String[0]))) {
                throw new IllegalArgumentException("NATS consumers have overlapping subject filters: " + filters);
            }
        }

        return natsConfig;
    }
}
