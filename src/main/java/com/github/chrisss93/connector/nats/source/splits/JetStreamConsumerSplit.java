package com.github.chrisss93.connector.nats.source.splits;

import com.github.chrisss93.connector.nats.source.NATSConsumerConfig;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class JetStreamConsumerSplit implements SourceSplit {
    private final String stream;
    private final NATSConsumerConfig config;
    private final Set<String> pendingAcks;

    public JetStreamConsumerSplit(String stream, ConsumerConfiguration config, Set<String> pendingAcks) {
        this.stream = stream;
        this.config = new NATSConsumerConfig(config);
        this.pendingAcks = pendingAcks;
    }

    public JetStreamConsumerSplit(String stream, ConsumerConfiguration config) {
        this(stream, config, new HashSet<>());
    }

    @Override
    public String splitId() {
        return String.format("%s > %s", getStream(), getName());
    }

    public Set<String> getPendingAcks() {
        return pendingAcks;
    }

    public String getStream() {
        return stream;
    }
    public ConsumerConfiguration getConfig() {
        return config.build();
    }

    public String getName() {
        return getConfig().getName();
    }

    public long getStartingSequence() {
        return getConfig().getStartSequence();
    }

    public static void write(JetStreamConsumerSplit split, ObjectOutputStream out) throws IOException {
        out.writeUTF(split.getStream());
        out.writeObject(split.config);
        out.writeInt(split.getPendingAcks().size());
        for (String ack : split.getPendingAcks()) {
            out.writeUTF(ack);
        }
    }

    public static JetStreamConsumerSplit read(ObjectInputStream in) throws IOException, ClassNotFoundException {
        String stream = in.readUTF();
        NATSConsumerConfig config = (NATSConsumerConfig) in.readObject();
        int ackCount = in.readInt();
        Set<String> acks = new HashSet<>(ackCount);
        for (int i = 0; i < ackCount; i++) {
            acks.add(in.readUTF());
        }
        return new JetStreamConsumerSplit(stream, config.build(), acks);
    }

    @Override
    public String toString() {
        return "JetStreamConsumerSplit{" +
            "split=" + splitId() +
            ", pendingAcks=" + pendingAcks.size() +
            ", config=" + printConfig() +
            "}";
    }

    private String printConfig() {
        ConsumerConfiguration conf = getConfig();

        String start = "";
        switch (conf.getDeliverPolicy()) {
            case ByStartSequence: start = ", startSeq=" + conf.getStartSequence(); break;
            case ByStartTime: start = ", startTime=" + conf.getStartTime(); break;
        }
        return "{filterSubject=" + conf.getFilterSubject() + start + ", ackPolicy=" + conf.getAckPolicy() + "}";
    }

    @Override
    public int hashCode() {
        ConsumerConfiguration conf = this.getConfig();
        return Objects.hash(this.stream, this.pendingAcks, conf.getDurable(), conf.getName(), conf.getFilterSubject(),
            conf.getDeliverPolicy(), conf.getDeliverGroup(), conf.getDeliverSubject(), conf.getAckWait(),
            conf.getBackoff(), conf.getDescription(), conf.getIdleHeartbeat(), conf.getInactiveThreshold(),
            conf.getSampleFrequency(), conf.getReplayPolicy(), conf.getMaxExpires(), conf.getStartSequence(),
            conf.getStartTime(), conf.getMaxPullWaiting(), conf.getMaxAckPending(), conf.getMaxBatch(),
            conf.getMaxBytes(), conf.getMaxDeliver(),conf.getMaxPullWaiting(), conf.getNumReplicas(),
            conf.getRateLimit());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof JetStreamConsumerSplit)) {
            return false;
        }
        JetStreamConsumerSplit otherSplit = (JetStreamConsumerSplit) obj;
        ConsumerConfiguration otherConfig = otherSplit.getConfig();
        ConsumerConfiguration myConfig = this.getConfig();

        return Objects.equals(this.stream, otherSplit.stream) &&
            this.pendingAcks.equals(otherSplit.pendingAcks) &&
            Objects.equals(myConfig.getDurable(), otherConfig.getDurable()) &&
            Objects.equals(myConfig.getName(), otherConfig.getName()) &&
            Objects.equals(myConfig.getFilterSubject(), otherConfig.getFilterSubject()) &&
            Objects.equals(myConfig.getDeliverPolicy(), otherConfig.getDeliverPolicy()) &&
            Objects.equals(myConfig.getDeliverGroup(), otherConfig.getDeliverGroup()) &&
            Objects.equals(myConfig.getDeliverSubject(), otherConfig.getDeliverSubject()) &&
            Objects.equals(myConfig.getAckWait(), otherConfig.getAckWait()) &&
            Objects.equals(myConfig.getBackoff(), otherConfig.getBackoff()) &&
            Objects.equals(myConfig.getDescription(), otherConfig.getDescription()) &&
            Objects.equals(myConfig.getIdleHeartbeat(), otherConfig.getIdleHeartbeat()) &&
            Objects.equals(myConfig.getInactiveThreshold(), otherConfig.getInactiveThreshold()) &&
            Objects.equals(myConfig.getSampleFrequency(), otherConfig.getSampleFrequency()) &&
            Objects.equals(myConfig.getReplayPolicy(), otherConfig.getReplayPolicy()) &&
            Objects.equals(myConfig.getMaxExpires(), otherConfig.getMaxExpires()) &&
            Objects.equals(myConfig.getStartTime(), otherConfig.getStartTime()) &&
            myConfig.getStartSequence() == otherConfig.getStartSequence() &&
            myConfig.getMaxPullWaiting() == otherConfig.getMaxPullWaiting() &&
            myConfig.getMaxAckPending() == otherConfig.getMaxAckPending() &&
            myConfig.getMaxBatch() == otherConfig.getMaxBatch() &&
            myConfig.getMaxBytes() == otherConfig.getMaxBytes() &&
            myConfig.getMaxDeliver() == otherConfig.getMaxDeliver() &&
            myConfig.getNumReplicas() == otherConfig.getNumReplicas() &&
            myConfig.getRateLimit() == otherConfig.getRateLimit()
            ;
    }
}