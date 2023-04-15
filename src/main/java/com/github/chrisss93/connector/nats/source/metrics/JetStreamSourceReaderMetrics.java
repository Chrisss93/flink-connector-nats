package com.github.chrisss93.connector.nats.source.metrics;

import com.github.chrisss93.connector.nats.common.NatsMetrics;
import io.nats.client.Connection;
import io.nats.client.Statistics;
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
    private final Connection connection;
    private final boolean advanced;
    private final MetricGroup statsGroup;
    private long lastBytesIn = 0;
    private long lastMessagesIn = 0;

    public JetStreamSourceReaderMetrics(Connection connection, SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.connection = connection;
        this.readerMetrics = sourceReaderMetricGroup;
        this.advanced = connection.getOptions().isTrackAdvancedStats();

        MetricGroup readerGroup = this.readerMetrics.addGroup(READER_GROUP);
        setServerMetrics(connection, readerGroup);
        this.statsGroup = readerGroup.addGroup(STATS_GROUP);
    }

    public void updateConsumerCount(int count) {
        if (!advanced) statsGroup.gauge(CONSUMER_COUNT_GAUGE, () -> count);
    }

    public void updateAcks(boolean success) {
        if (!advanced) statsGroup.counter(success ? ACK_SUCCESS_COUNTER : ACK_FAILURE_COUNTER).inc();
    }

    public void updateMetrics() {
        if (advanced) {
            NatsMetrics metrics = new NatsMetrics(connection);
            metrics.addToMetricGroup(statsGroup);
            updateBytesIn(metrics.getBytesIn());
//            updateMessagesIn(metrics.getMessagesIn() - metrics.getPingsSent() - metrics.getRequestsSent());
        } else {
            Statistics stats = connection.getStatistics();
            updateBytesIn(stats.getInBytes());
//            updateMessagesIn(stats.getInMsgs());
            updateReconnects(stats);
        }
    }

    private void updateBytesIn(long current) {
        readerMetrics.getIOMetricGroup().getNumBytesInCounter().inc(current - lastBytesIn);
        lastBytesIn = current;
    }

    private void updateMessagesIn(long current) {
        readerMetrics.getIOMetricGroup().getNumRecordsInCounter().inc(current - lastMessagesIn);
        lastMessagesIn = current;
    }

    private void updateReconnects(Statistics stats) {
        statsGroup.gauge(RECONNECTS, stats::getReconnects);
    }
}
