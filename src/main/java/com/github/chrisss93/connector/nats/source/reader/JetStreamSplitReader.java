package com.github.chrisss93.connector.nats.source.reader;

import com.github.chrisss93.connector.nats.source.enumerator.offsets.StopRule;
import com.github.chrisss93.connector.nats.source.metrics.JetStreamSourceReaderMetrics;
import com.github.chrisss93.connector.nats.source.splits.JetStreamConsumerSplit;
import com.github.chrisss93.connector.nats.source.splits.SplitsRemoval;
import io.nats.client.*;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.impl.AckType;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JetStreamSplitReader implements SplitReader<Message, JetStreamConsumerSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSplitReader.class);
    private static final int BATCH_SIZE = 5000;
    private static final long TIMEOUT_MS = 10000L;

    private final Connection connection;
    private final JetStreamManagement jsm;
    private final StopRule stopRule;
    private final JetStreamSourceReaderMetrics readerMetrics;
    private final Map<String, JetStreamSubscription> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, StopRule> stopRules = new HashMap<>();
    private final Map<String, Boolean> shutdownSubscription = new HashMap<>();

    public JetStreamSplitReader(Options connectOptions, StopRule stopRule, SourceReaderMetricGroup metricGroup) {
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
        this.readerMetrics = new JetStreamSourceReaderMetrics(connection, metricGroup);
    }

    @Override
    public RecordsWithSplitIds<Message> fetch() {
        RecordsBySplits.Builder<Message> recordsWithSplits = new RecordsBySplits.Builder<>();

        subscriptions.forEach( (splitId, subscription) -> {
            if (shutdownSubscription.containsKey(splitId) && shutdownSubscription.remove(splitId)) {
                LOG.info("Force-completing split {}", splitId);
                recordsWithSplits.addFinishedSplit(splitId);
                return;
            }
            if (subscription.isActive()) {
                int i = 0;
                try {
                    Iterator<Message> messages = subscription.iterate(BATCH_SIZE, TIMEOUT_MS);
                    while (messages.hasNext()) {
                        Message message = messages.next();
                        i++;
                        recordsWithSplits.add(splitId, message);
                        if (stopRules.get(splitId).shouldStop(message)) {
                            recordsWithSplits.addFinishedSplits(subscriptions.keySet());
                            break;
                        }
                    }
                } catch (IllegalStateException e) {
                    LOG.warn("Split reader for {} has been paused.", splitId);
                }
                LOG.debug("Fetched {} new messages from split {}", i, splitId);
            }
        });
        readerMetrics.updateMetrics();
        return recordsWithSplits.build();
    }

    public void acknowledge(Set<String> replyTos, boolean doubleAck) {
        for (String r : replyTos) {
            if (!doubleAck) {
                connection.publish(r, AckType.AckAck.bytes);
                readerMetrics.updateAcks(true);
            } else {
                try {
                    Message reply = connection.request(r, AckType.AckAck.bytes, Duration.ofSeconds(1L));
                    readerMetrics.updateAcks(reply != null);
                    if (reply == null) {
                        LOG.warn("NATS did not confirm message acks. Trying again at next checkpoint.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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

        if (splitsChanges instanceof SplitsRemoval) {
            splitsChanges.splits().forEach(s -> shutdownSubscription.put(s.splitId(), true));
            readerMetrics.updateConsumerCount(subscriptions.size() - shutdownSubscription.size());
            return;
        }

        splitsChanges.splits().forEach( s -> {
            try {
                stopRules.put(s.splitId(), stopRule.create(jsm.getStreamInfo(s.getStream())));
            } catch (IOException | JetStreamApiException e) {
                throw new FlinkRuntimeException("Can't create stop-rule for split: " + s.splitId(), e);
            }
            createNatsConsumer(s);
            subscriptions.put(s.splitId(), subscribe(s.getStream(), s.getName()));
        });

        readerMetrics.updateConsumerCount(subscriptions.size());
    }

    @Override
    public void wakeUp() {
        subscriptions.forEach((splitId, subscription) -> {
            if (!subscription.isActive()) {
                return;
            }
            LOG.debug("Draining subscription to {} and resubscribing", splitId);
            try {
                subscription.drain(Duration.ofSeconds(1));
                ConsumerInfo info = subscription.getConsumerInfo();
                subscriptions.put(splitId, subscribe(info.getStreamName(), info.getName()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException("Failed to wakeup split-reader for split: " + splitId, e);
            } catch (JetStreamApiException | IOException e) {
                throw new FlinkRuntimeException("Failed to wakeup split-reader for split: " + splitId, e);
            }
        });
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

        splitsToPause.forEach(s -> {
            JetStreamSubscription subscription = subscriptions.get(s.splitId());
            if (subscription == null) {
                throw new IllegalStateException("Split " + s.splitId() + " is not assigned to this reader.");
            }
            try {
                subscription.drain(Duration.ofSeconds(5L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException("Can't pause split: " + s.splitId(), e);
            }
        });

        splitsToResume.forEach(s -> {
            if (!subscriptions.containsKey(s.splitId())) {
                throw new IllegalStateException("Split " + s.splitId() + " is not assigned to this reader.");
            }
            subscriptions.put(s.splitId(), subscribe(s.getStream(), s.getName()));
        });
    }

    private void createNatsConsumer(JetStreamConsumerSplit split) {
        LOG.info("Creating new NATS consumer {}", split.splitId());
        try {
            if (jsm.getConsumerNames(split.getStream()).contains(split.getName())) {
                jsm.deleteConsumer(split.getStream(), split.getName());
            }
            jsm.addOrUpdateConsumer(split.getStream(), split.getConfig());
        } catch (JetStreamApiException | IOException e) {
            throw new FlinkRuntimeException("Failed to create/update NATS consumer for split: " + split.splitId(), e);
        }
    }

    private JetStreamSubscription subscribe(String streamName, String consumerName) {
        try {
            return connection.jetStream().subscribe(null, PullSubscribeOptions.bind(streamName, consumerName));
        } catch (JetStreamApiException | IOException e) {
            throw new FlinkRuntimeException(
                String.format("Failed to subscribe to NATS consumer: %s on stream: %s", streamName, consumerName),
                e
            );
        }
    }
}
