package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.enumerator.offsets.StopRule;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import io.nats.client.*;
import io.nats.client.impl.AckType;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static io.nats.client.support.NatsJetStreamConstants.JS_CONSUMER_NOT_FOUND_ERR;

public class JetStreamConsumerSplitReader implements SplitReader<Message, JetStreamConsumerSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(JetStreamConsumerSplitReader.class);
    private static final int BATCH_SIZE = 256;
    private static final long TIMEOUT_MS = 5000L;

    private final Connection connection;
    private final JetStreamManagement jsm;
    private final StopRule stopRule;
    private StopRule stopper;
    private JetStreamSubscription subscription;
    private JetStreamConsumerSplit currentSplit;

    public JetStreamConsumerSplitReader(Options connectOptions, StopRule stopRule) {
        this.stopRule = stopRule;
        try {
            connection = Nats.connect(connectOptions);
            jsm = connection.jetStreamManagement();
        } catch (IOException e) {
            throw new FlinkRuntimeException("Can't connect to NATS", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlinkRuntimeException("Can't connect to NATS", e);
        }
    }

    @Override
    public RecordsWithSplitIds<Message> fetch() {
        RecordsBySplits.Builder<Message> recordsWithSplits = new RecordsBySplits.Builder<>();

        if (subscription != null && subscription.isActive()) {
            int i = 0;
            try {
                Iterator<Message> messages = subscription.iterate(BATCH_SIZE, TIMEOUT_MS);
                while (messages.hasNext()) {
                    Message message = messages.next();
                    if (stopper.shouldStop(message)) {
                        recordsWithSplits.addFinishedSplit(currentSplit.splitId());
                        break;
                    }
                    recordsWithSplits.add(currentSplit.splitId(), message);
                    i++;
                }
            } catch (IllegalStateException e) {
                LOG.info("Split reader for {} has been paused.", currentSplit.splitId());
                return recordsWithSplits.build();
            }

            LOG.debug("Fetched {} new messages from split {}", i, currentSplit.splitId());
            System.out.printf("Fetched %d new messages from split %s%n", i, currentSplit.splitId());
        }

        return recordsWithSplits.build();
    }

    public void acknowledge(Set<String> replyTos, boolean doubleAck) {
        for (String r : replyTos) {
            if (!doubleAck) {
                connection.publish(r, AckType.AckAck.bytes);
            } else {
                try {
                    if (connection.request(r, AckType.AckAck.bytes, Duration.ofSeconds(1L)) != null) {
                        LOG.warn("NATS did not confirm message acks. Trying again at next checkpoint.");
                    }
                } catch (InterruptedException e) {
                    LOG.warn("NATS did not confirm message acks. Trying again at next checkpoint.");
                }
            }
        }

        if (!doubleAck) {
            try {
                connection.flushBuffer();
            } catch (IOException e) {
                throw new FlinkRuntimeException("Can't flush outgoing message queue after sending acks", e);
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<JetStreamConsumerSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format("The SplitChange type of %s is not supported.", splitsChanges.getClass())
            );
        }
        if (subscription != null) {
            throw new IllegalStateException("This split reader has already been assigned a split.");
        }
        Preconditions.checkArgument(
                splitsChanges.splits().size() == 1,
                "JetStreamSplitReader only supports one split."
        );

        currentSplit = splitsChanges.splits().get(0);
        try {
            stopper = stopRule.create(jsm.getStreamInfo(currentSplit.getStream()));
        } catch (JetStreamApiException | IOException e) {
            throw new FlinkRuntimeException("Can't initialize stopping rule", e);
        }
        maybeCreateNatsConsumer();
        subscribe();
    }

    @Override
    public void wakeUp() {
        if (subscription == null || !subscription.isActive()) {
            return;
        }
        LOG.debug("Draining the subscription and resubscribing for split: " + currentSplit.splitId());
        System.out.println("Draining the subscription and resubscribing for split " + currentSplit.splitId());
        try {
            subscription.drain(Duration.ofSeconds(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlinkRuntimeException("Failed to wakeup split-reader for split: " + currentSplit.splitId(), e);
        }
        subscribe();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    public void pauseOrResumeSplits(
        Collection<JetStreamConsumerSplit> splitsToPause,
        Collection<JetStreamConsumerSplit> splitsToResume) {

        Preconditions.checkState(
            splitsToPause.size() + splitsToResume.size() <= 1,
            "This NATS split reader only supports one split.");

        if (!splitsToResume.isEmpty()) {
            subscribe();
        } else if (!splitsToPause.isEmpty()) {
            try {
                subscription.drain(Duration.ofSeconds(5L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException("Can't pause split-reader for split: " + currentSplit.splitId(), e);
            }
        }
    }

    private void maybeCreateNatsConsumer() {
        try {
            jsm.getConsumerInfo(currentSplit.getStream(), currentSplit.getName());
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                "Can't subscribe to NATS stream > consumer %s" + currentSplit.splitId(), e
            );
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() != JS_CONSUMER_NOT_FOUND_ERR) {
                throw new FlinkRuntimeException(
                    "Can't subscribe to NATS stream > consumer %s" + currentSplit.splitId(), e
                );
            }
            createNatsConsumer();
        }
    }

    private void createNatsConsumer() {
        LOG.info("Creating new NATS consumer {}", currentSplit.splitId());
        System.out.printf("Creating new NATS consumer %s.%n", currentSplit.splitId());

        try {
            jsm.addOrUpdateConsumer(currentSplit.getStream(), currentSplit.getConfig());
        } catch (JetStreamApiException | IOException e) {
            throw new FlinkRuntimeException("Failed to create NATS consumer for split: " + currentSplit.splitId(), e);
        }
    }

    private void subscribe() {
        try {
            subscription = connection.jetStream().subscribe(null,
                PullSubscribeOptions.bind(currentSplit.getStream(), currentSplit.getName())
            );
        } catch (JetStreamApiException | IOException e) {
            throw new FlinkRuntimeException("Failed to subscribe to NATS consumer: " + currentSplit.splitId(), e);
        }
    }
}
