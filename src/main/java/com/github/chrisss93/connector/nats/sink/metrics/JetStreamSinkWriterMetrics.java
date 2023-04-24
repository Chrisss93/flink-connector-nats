package com.github.chrisss93.connector.nats.sink.metrics;

import com.github.chrisss93.connector.nats.common.NATSMetrics;
import io.nats.client.Connection;
import io.nats.client.Statistics;
import io.nats.client.api.PublishAck;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.util.concurrent.atomic.AtomicLong;

import static com.github.chrisss93.connector.nats.common.MetricUtils.*;

public class JetStreamSinkWriterMetrics {

    private static final String WRITER_GROUP = "jetStreamSinkWriter";

    private final SinkWriterMetricGroup writerMetrics;
    private final boolean advanced;
    private final Statistics statistics;
    private final Counter ackFailCounter;
    private long lastBytesOut = 0;
    private long lastMessagesOut = 0;
    private Counter duplicateCounter;


    public JetStreamSinkWriterMetrics(Connection connection, SinkWriterMetricGroup sinkWriterMetricGroup,
                                      AtomicLong inFlightNumber) {

        this.writerMetrics = sinkWriterMetricGroup;
        this.advanced = connection.getOptions().isTrackAdvancedStats();
        this.statistics = connection.getStatistics();

        MetricGroup writerGroup = this.writerMetrics.addGroup(WRITER_GROUP);
        registerServerMetrics(connection, this.writerMetrics);
        MetricGroup statsGroup = writerGroup.addGroup(STATS_GROUP);
        ackFailCounter = statsGroup.counter(ACK_FAILURE_COUNTER);

        if (advanced) {
            NATSMetrics.registerMetrics(statsGroup, statistics);
            statsGroup.gauge(ACK_SUCCESS_COUNTER, () ->
                new NATSMetrics(statistics).getRepliesReceived() - ackFailCounter.getCount()
            );
        } else {
            statsGroup.gauge(RECONNECTS, statistics::getReconnects);
            statsGroup.gauge(OUTSTANDING_REQUEST_FUTURES, inFlightNumber::get);
            duplicateCounter = statsGroup.counter(DUPLICATE_REPLIES_RECEIVED);
        }
    }

    public void updateAcks(PublishAck ack) {
        if (ack == null) {
            ackFailCounter.inc();
        } else if (!advanced && ack.isDuplicate()) {
            duplicateCounter.inc();
        }
    }

    public void update() {
        updateBytesOut();
        updateMessagesOut();
    }

    /*
    Somewhat confusingly, use getInMsgs and getInBytes from nats client statistics, not getOutMsgs and
    getOutBytes since those include messages from the protocol initiation + PING/PONG (during keep-alive
    and flushes). getInMsgs and getInBytes refer to the "real" NATS messages pushed to the nats-client's
    internal queue in preparation for being published.
    */
    private void updateBytesOut() {
        long current = statistics.getInBytes();
        writerMetrics.getNumBytesSendCounter().inc(current - lastBytesOut);
        lastBytesOut = current;
    }

    private void updateMessagesOut() {
        long current = statistics.getInMsgs();
        writerMetrics.getNumRecordsSendCounter().inc(current - lastMessagesOut);
        lastMessagesOut = current;
    }
}
