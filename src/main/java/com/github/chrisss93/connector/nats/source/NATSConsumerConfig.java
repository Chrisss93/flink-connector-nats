package com.github.chrisss93.connector.nats.source;

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

/**
 *     A serializable version of java-nats {@link ConsumerConfiguration.Builder}
 */
public class NATSConsumerConfig extends ConsumerConfiguration.Builder implements Serializable {

    public NATSConsumerConfig(ConsumerConfiguration cc) {
        super(cc);
    }

    private void writeObject(ObjectOutputStream output) throws IOException {
        output.writeUTF(this.build().toJson());
    }

    private void readObject(ObjectInputStream input) throws IOException {
        JsonValue v = JsonParser.parse(input.readUTF());

        this.deliverPolicy(DeliverPolicy.get(readString(v, DELIVER_POLICY)))
            .ackPolicy(AckPolicy.get(readString(v, ACK_POLICY)))
            .replayPolicy(ReplayPolicy.get(readString(v, REPLAY_POLICY)))
            .description(readString(v, DESCRIPTION))
            .durable(readString(v, DURABLE_NAME))
            .name(readString(v, NAME))
            .deliverSubject(readString(v, DELIVER_SUBJECT))
            .deliverGroup(readString(v, DELIVER_GROUP))
            .filterSubject(readString(v, FILTER_SUBJECT))
            .sampleFrequency(readString(v, SAMPLE_FREQ))
            .startTime(readDate(v, OPT_START_TIME))
            .ackWait(readNanos(v, ACK_WAIT))
            .idleHeartbeat(readNanos(v, IDLE_HEARTBEAT))
            .maxExpires(readNanos(v, MAX_EXPIRES))
            .inactiveThreshold(readNanos(v, INACTIVE_THRESHOLD))
            .startSequence(readLong(v, OPT_START_SEQ))
            .maxDeliver(readLong(v, MAX_DELIVER))
            .rateLimit(readLong(v, RATE_LIMIT_BPS))
            .maxAckPending(readLong(v, MAX_ACK_PENDING))
            .maxPullWaiting(readLong(v, MAX_WAITING))
            .maxBatch(readLong(v, MAX_BATCH))
            .maxBytes(readLong(v, MAX_BYTES))
            .numReplicas(readInteger(v, NUM_REPLICAS))
            .headersOnly(readBoolean(v, HEADERS_ONLY, null))
            .memStorage(readBoolean(v, MEM_STORAGE, null));

        if (readBoolean(v, FLOW_CONTROL, false)) {
            this.flowControl(readNanos(v, IDLE_HEARTBEAT));
        }
    }

    public static NATSConsumerConfig of(Map<String, String> m) {
        NATSConsumerConfig config = new NATSConsumerConfig(ConsumerConfiguration.builder().build());

        if (m.containsKey(DELIVER_POLICY)) config.deliverPolicy(DeliverPolicy.get(m.get(DELIVER_POLICY)));
        if (m.containsKey(ACK_POLICY)) config.ackPolicy(AckPolicy.get(m.get(ACK_POLICY)));
        if (m.containsKey(REPLAY_POLICY)) config.replayPolicy(ReplayPolicy.get(m.get(REPLAY_POLICY)));
        if (m.containsKey(DESCRIPTION)) config.description(m.get(DESCRIPTION));
        if (m.containsKey(DURABLE_NAME)) config.durable(m.get(DURABLE_NAME));
        if (m.containsKey(NAME)) config.name(m.get(NAME));
        if (m.containsKey(DELIVER_SUBJECT)) config.deliverSubject(m.get(DELIVER_SUBJECT));
        if (m.containsKey(DELIVER_GROUP)) config.deliverGroup(m.get(DELIVER_GROUP));
        if (m.containsKey(FILTER_SUBJECT)) config.filterSubject(m.get(FILTER_SUBJECT));
        if (m.containsKey(SAMPLE_FREQ)) config.sampleFrequency(m.get(SAMPLE_FREQ));
        if (m.containsKey(OPT_START_TIME)) config.startTime(DateTimeUtils.parseDateTimeThrowParseError(m.get(OPT_START_TIME)));
        if (m.containsKey(ACK_WAIT)) config.ackWait(Duration.ofNanos(parseLong(m.get(ACK_WAIT))));
        if (m.containsKey(IDLE_HEARTBEAT)) config.idleHeartbeat(Duration.ofNanos(parseLong(m.get(IDLE_HEARTBEAT))));
        if (m.containsKey(MAX_EXPIRES)) config.maxExpires(Duration.ofNanos(parseLong(m.get(MAX_EXPIRES))));
        if (m.containsKey(INACTIVE_THRESHOLD)) config.inactiveThreshold(Duration.ofNanos(parseLong(m.get(INACTIVE_THRESHOLD))));
        if (m.containsKey(OPT_START_SEQ)) config.startSequence(parseLong(m.get(OPT_START_SEQ)));
        if (m.containsKey(MAX_DELIVER)) config.maxDeliver(parseLong(m.get(MAX_DELIVER)));
        if (m.containsKey(RATE_LIMIT_BPS)) config.rateLimit(parseLong(m.get(RATE_LIMIT_BPS)));
        if (m.containsKey(MAX_ACK_PENDING)) config.maxAckPending(parseLong(m.get(MAX_ACK_PENDING)));
        if (m.containsKey(MAX_WAITING)) config.maxPullWaiting(parseLong(m.get(MAX_WAITING)));
        if (m.containsKey(MAX_BATCH)) config.maxBatch(parseLong(m.get(MAX_BATCH)));
        if (m.containsKey(MAX_BYTES)) config.maxBytes(parseLong(m.get(MAX_BYTES)));
        if (m.containsKey(NUM_REPLICAS)) config.numReplicas(Integer.parseInt(m.get(NUM_REPLICAS)));
        if (m.containsKey(HEADERS_ONLY)) config.headersOnly(Boolean.parseBoolean(m.get(HEADERS_ONLY)));
        if (m.containsKey(MEM_STORAGE)) config.memStorage(Boolean.parseBoolean(m.get(MEM_STORAGE)));
        if (m.containsKey(FLOW_CONTROL)) config.flowControl(Duration.ofNanos(Long.parseLong(m.get(IDLE_HEARTBEAT))));

        return config;
    }

    private static long parseLong(String str) {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            return Double.valueOf(str).longValue();
        }
    }
}
