package com.github.chrisss93.connector.nats.source.metrics;

import com.github.chrisss93.connector.nats.common.NATSMetrics;
import io.nats.client.Connection;
import io.nats.client.Statistics;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import static com.github.chrisss93.connector.nats.common.MetricUtils.*;

/**
 * This class provides static metrics about the NATS server that the client is talking to as well as internal
 * NATS client statistics. It will populate the {@link OperatorIOMetricGroup#getNumBytesInCounter()} but
 * <strong>not</strong> the {@link OperatorIOMetricGroup#getNumRecordsInCounter()}, since this class is meant to be
 * used with {@link org.apache.flink.connector.base.source.reader.SourceReaderBase SourceReaderBase}, and that
 * class already populates that metric. We'll avoid double counting this way.
 */
public class JetStreamSourceReaderMetrics {

    private static final String READER_GROUP = "jetStreamSourceReader";
    private static final String CONSUMER_COUNT_GAUGE = "consumerCount";

    private final SourceReaderMetricGroup readerMetrics;
    private final boolean advanced;
    private final Statistics statistics;
    private final Counter consumerCounter;
    private long lastBytesIn = 0;
    private long lastMessagesIn = 0;
    private Counter ackSuccessCounter;
    private Counter ackFailCounter;

    public JetStreamSourceReaderMetrics(Connection connection, SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.readerMetrics = sourceReaderMetricGroup;
        this.advanced = connection.getOptions().isTrackAdvancedStats();
        this.statistics = connection.getStatistics();

        MetricGroup readerGroup = this.readerMetrics.addGroup(READER_GROUP);
        registerServerMetrics(connection, readerGroup);
        MetricGroup statsGroup = readerGroup.addGroup(STATS_GROUP);
        consumerCounter = statsGroup.counter(CONSUMER_COUNT_GAUGE);

        if (advanced) {
            NATSMetrics.registerMetrics(statsGroup, statistics);
        } else {
            statsGroup.gauge(RECONNECTS, statistics::getReconnects);
            statsGroup.gauge(DROPPED_MESSAGES, statistics::getDroppedCount);
            ackSuccessCounter = statsGroup.counter(ACK_SUCCESS_COUNTER);
            ackFailCounter = statsGroup.counter(ACK_FAILURE_COUNTER);
        }
    }

    public Counter getConsumerCount() {
        return consumerCounter;
    }

    public void updateAcks(boolean success) {
        if (advanced) return;
        if (success) {
            ackSuccessCounter.inc();
        } else {
            ackFailCounter.inc();
        }
    }

    public void update() {
        updateBytesIn();
//        updateMessagesIn();
    }

    private void updateBytesIn() {
        long current = statistics.getInBytes();
        readerMetrics.getIOMetricGroup().getNumBytesInCounter().inc(current - lastBytesIn);
        lastBytesIn = current;
    }

    private void updateMessagesIn() {
        long current = statistics.getInMsgs();
        readerMetrics.getIOMetricGroup().getNumRecordsInCounter().inc(current - lastMessagesIn);
        lastMessagesIn = current;
    }
}
