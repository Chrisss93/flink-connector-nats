package com.github.chrisss93.connector.nats.sink.metrics;

import com.github.chrisss93.connector.nats.common.NatsMetrics;
import io.nats.client.Connection;
import io.nats.client.Statistics;
import io.nats.client.api.PublishAck;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.util.concurrent.atomic.AtomicLong;

import static com.github.chrisss93.connector.nats.common.MetricUtils.*;

public class JetStreamSinkWriterMetrics {

    private static final String WRITER_GROUP = "jetStreamSinkWriter";

    private final SinkWriterMetricGroup writerMetrics;
    private final Connection connection;
    private final MetricGroup statsGroup;
    private final boolean advanced;
    private long lastBytesOut = 0;
    private long lastMessagesOut = 0;

    public JetStreamSinkWriterMetrics(Connection connection, SinkWriterMetricGroup sinkWriterMetricGroup) {
        this.connection = connection;
        this.writerMetrics = sinkWriterMetricGroup;
        this.advanced = connection.getOptions().isTrackAdvancedStats();

        MetricGroup writerGroup = this.writerMetrics.addGroup(WRITER_GROUP);
        setServerMetrics(connection, this.writerMetrics);
        this.statsGroup = writerGroup.addGroup(STATS_GROUP);
    }

    public void updateAcks(PublishAck ack) {
        if (ack == null) {
            statsGroup.counter(ACK_FAILURE_COUNTER).inc();
        } else if (!advanced && ack.isDuplicate()) {
            statsGroup.counter(DUPLICATE_REPLIES_RECEIVED).inc();
        }
    }
    public void updateInFlightRequests(AtomicLong count) {
        if (!advanced) statsGroup.gauge(OUTSTANDING_REQUEST_FUTURES, count::get);
    }

    public void updateMetrics() {
        /*
            Somewhat confusingly, use getInMsgs and getInBytes from nats client statistics, not getOutMsgs and
            getOutBytes since those include messages from the protocol initiation + PING/PONG (during keep-alive
            and flushes). getInMsgs and getInBytes refer to the "real" NATS messages pushed to the nats-client's
            internal queue in preparation for being published.
         */
        if (advanced) {
            NatsMetrics metrics = new NatsMetrics(connection);
            metrics.addToMetricGroup(statsGroup);
            updateBytesOut(metrics.getBytesIn());
            updateMessagesOut(metrics.getMessagesIn());
            statsGroup.gauge(ACK_SUCCESS_COUNTER,
                () -> metrics.getRepliesReceived() - statsGroup.counter(ACK_FAILURE_COUNTER).getCount()
            );
        } else {
            Statistics stats = connection.getStatistics();
            updateBytesOut(stats.getInBytes());
            updateMessagesOut(stats.getInMsgs());
            updateReconnects(stats);
        }
    }

    private void updateBytesOut(long current) {
        writerMetrics.getNumBytesSendCounter().inc(current - lastBytesOut);
        lastBytesOut = current;
    }

    private void updateMessagesOut(long current) {
        writerMetrics.getNumRecordsSendCounter().inc(current - lastMessagesOut);
        lastMessagesOut = current;
    }

    private void updateReconnects(Statistics stats) {
        statsGroup.gauge(RECONNECTS, stats::getReconnects);
    }
}
