package com.github.chrisss93.connector.nats.source;

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

/**
 *     A serializable version of java-nats {@link ConsumerConfiguration.Builder}
 */
public class NATSConsumerConfig extends ConsumerConfiguration.Builder implements Serializable {
    protected NATSConsumerConfig(ConsumerConfiguration cc) {
        super(cc);
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
    private void writeObject(ObjectOutputStream output) throws IOException {
        output.writeUTF(this.build().toJson());
    }
}
