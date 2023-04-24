package com.github.chrisss93.connector.nats.common;

import io.nats.client.Connection;
import io.nats.client.Statistics;
import org.apache.flink.metrics.MetricGroup;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class NATSMetrics implements Statistics {
    private static final String OKS_RECEIVED = "OKsReceived";
    private static final String AVERAGE_BYTES_PER_WRITE = "AverageBytesPerWrite";
    private static final String HANDLED_EXCEPTIONS = "HandledExceptions";
    private static final String MAX_BYTES_PER_WRITE = "MaxBytesPerWrite";
    private static final String MIN_BYTES_PER_WRITE = "MinBytesPerWrite";
    private static final String SOCKET_READS = "SocketReads";
    private static final String MIN_BYTES_PER_READ = "MinBytesPerRead";
    private static final String MESSAGES_OUT = "Messagesout";
    private static final String REPLIES_RECEIVED = "RepliesReceived";
    private static final String AVERAGE_BYTES_PER_READ = "AverageBytesPerRead";
    private static final String PINGS_SENT = "PingsSent";
    private static final String MAX_BYTES_PER_READ = "MaxBytesPerRead";
    private static final String BYTES_OUT = "Bytesout";
    private static final String ORPHAN_REPLIES_RECEIVED = "OrphanRepliesReceived";
    private static final String BYTES_IN = "Bytesin";
    private static final String REQUESTS_SENT = "RequestsSent";
    private static final String SUCCESSFUL_FLUSH_CALLS = "SuccessfulFlushCalls";
    private static final String MESSAGES_IN = "Messagesin";
    private static final String SOCKET_WRITES = "SocketWrites";
    private static final String ERRS_RECEIVED = "ErrsReceived";
    
    private final long oksReceived;
    private final float averageBytesPerWrite;
    private final long handledExceptions;
    private final long outstandingRequestFutures;
    private final long maxBytesPerWrite;
    private final long minBytesPerWrite;
    private final long socketReads;
    private final long droppedMessages;
    private final long minBytesPerRead;
    private final long messagesOut;
    private final long repliesReceived;
    private final float averageBytesPerRead;
    private final long pingsSent;
    private final long maxBytesPerRead;
    private final long bytesOut;
    private final long orphanRepliesReceived;
    private final long bytesIn;
    private final long requestsSent;
    private final long duplicateRepliesReceived;
    private final long successfulFlushCalls;
    private final long messagesIn;
    private final long reconnects;
    private final long socketWrites;
    private final long errsReceived;

        /*
        The package-private class io.nats.client.impl.NatsStatistics records much more detailed telemetry than the
        public interface io.nats.client.Statistics that it implements. There is no real way to access the internal
        implementation's extra info except through its toString method, so we have to go about arduously parsing
        a large string. I don't expect this will be stable across client library updates...
     */
    public NATSMetrics(Statistics stats) {
        Map<String, Number> m = Arrays.stream(stats.toString().split("\\n"))
            .filter(s -> !s.startsWith("#") && s.length() > 0)
            .collect(Collectors.toMap(
                s -> s.split(":")[0].replaceAll("[^a-zA-Z]", ""),
                s -> {
                    try {
                        return NumberFormat.getNumberInstance().parse(s.split(":\\s+")[1]);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            ));
        oksReceived = m.get(OKS_RECEIVED).longValue();
        averageBytesPerWrite = m.get(AVERAGE_BYTES_PER_WRITE).floatValue();
        handledExceptions = m.get(HANDLED_EXCEPTIONS).longValue();
        outstandingRequestFutures = m.get(MetricUtils.OUTSTANDING_REQUEST_FUTURES).longValue();
        maxBytesPerWrite = m.get(MAX_BYTES_PER_WRITE).longValue();
        minBytesPerWrite = m.get(MIN_BYTES_PER_WRITE).longValue();
        socketReads = m.get(SOCKET_READS).longValue();
        droppedMessages = m.get(MetricUtils.DROPPED_MESSAGES).longValue();
        minBytesPerRead = m.get(MIN_BYTES_PER_READ).longValue();
        messagesOut = m.get(MESSAGES_OUT).longValue();
        repliesReceived = m.get(REPLIES_RECEIVED).longValue();
        averageBytesPerRead = m.get(AVERAGE_BYTES_PER_READ).floatValue();
        pingsSent = m.get(PINGS_SENT).longValue();
        maxBytesPerRead = m.get(MAX_BYTES_PER_READ).longValue();
        bytesOut = m.get(BYTES_OUT).longValue();
        orphanRepliesReceived = m.get(ORPHAN_REPLIES_RECEIVED).longValue();
        bytesIn = m.get(BYTES_IN).longValue();
        requestsSent = m.get(REQUESTS_SENT).longValue();
        duplicateRepliesReceived = m.get(MetricUtils.DUPLICATE_REPLIES_RECEIVED).longValue();
        successfulFlushCalls = m.get(SUCCESSFUL_FLUSH_CALLS).longValue();
        messagesIn = m.get(MESSAGES_IN).longValue();
        reconnects = m.get(MetricUtils.RECONNECTS).longValue();
        socketWrites = m.get(SOCKET_WRITES).longValue();
        errsReceived = m.get(ERRS_RECEIVED).longValue();
    }

    public static void registerMetrics(MetricGroup group, Statistics stats) {
        group.gauge(OKS_RECEIVED, new NATSMetrics(stats)::getOksReceived);
        group.gauge(AVERAGE_BYTES_PER_WRITE, new NATSMetrics(stats)::getAverageBytesPerWrite);
        group.gauge(HANDLED_EXCEPTIONS, new NATSMetrics(stats)::getHandledExceptions);
        group.gauge(MAX_BYTES_PER_WRITE, new NATSMetrics(stats)::getMaxBytesPerWrite);
        group.gauge(MIN_BYTES_PER_WRITE, new NATSMetrics(stats)::getMinBytesPerWrite);
        group.gauge(SOCKET_READS, new NATSMetrics(stats)::getSocketReads);
        group.gauge(MIN_BYTES_PER_READ, new NATSMetrics(stats)::getMinBytesPerRead);
        group.gauge(REPLIES_RECEIVED, new NATSMetrics(stats)::getRepliesReceived);
        group.gauge(AVERAGE_BYTES_PER_READ, new NATSMetrics(stats)::getAverageBytesPerRead);
        group.gauge(PINGS_SENT, new NATSMetrics(stats)::getPingsSent);
        group.gauge(MAX_BYTES_PER_READ, new NATSMetrics(stats)::getMaxBytesPerRead);
        group.gauge(ORPHAN_REPLIES_RECEIVED, new NATSMetrics(stats)::getOrphanRepliesReceived);
        group.gauge(REQUESTS_SENT, new NATSMetrics(stats)::getRequestsSent);
        group.gauge(SUCCESSFUL_FLUSH_CALLS, new NATSMetrics(stats)::getSuccessfulFlushCalls);
        group.gauge(SOCKET_WRITES, new NATSMetrics(stats)::getSocketWrites);
        group.gauge(ERRS_RECEIVED, new NATSMetrics(stats)::getErrsReceived);

        group.gauge(MetricUtils.OUTSTANDING_REQUEST_FUTURES, new NATSMetrics(stats)::getOutstandingRequestFutures);
        group.gauge(MetricUtils.DUPLICATE_REPLIES_RECEIVED, new NATSMetrics(stats)::getDuplicateRepliesReceived);
        group.gauge(MetricUtils.RECONNECTS, new NATSMetrics(stats)::getReconnects);
        group.gauge(MetricUtils.DROPPED_MESSAGES, new NATSMetrics(stats)::getDroppedCount);
    }

    public long getOksReceived() {
	    return oksReceived;
	}
    public float getAverageBytesPerWrite() {
	    return averageBytesPerWrite;
	}
    public long getHandledExceptions() {
	    return handledExceptions;
	}
    public long getOutstandingRequestFutures() {
	    return outstandingRequestFutures;
	}
    public long getMaxBytesPerWrite() {
	    return maxBytesPerWrite;
	}
    public long getMinBytesPerWrite() {
	    return minBytesPerWrite;
	}
    public long getSocketReads() {
	    return socketReads;
	}
    public long getMinBytesPerRead() {
	    return minBytesPerRead;
	}
    public long getRepliesReceived() {
	    return repliesReceived;
	}
    public float getAverageBytesPerRead() {
	    return averageBytesPerRead;
	}
    public long getPingsSent() {
	    return pingsSent;
	}
    public long getMaxBytesPerRead() {
	    return maxBytesPerRead;
	}
    public long getOrphanRepliesReceived() {
	    return orphanRepliesReceived;
	}
    public long getRequestsSent() {
	    return requestsSent;
	}
    public long getDuplicateRepliesReceived() {
	    return duplicateRepliesReceived;
	}
    public long getSuccessfulFlushCalls() {
	    return successfulFlushCalls;
	}
    public long getSocketWrites() {
	    return socketWrites;
	}
    public long getErrsReceived() {
	    return errsReceived;
	}

    @Override
    public long getOutBytes() {
        return bytesOut;
    }
    @Override
    public long getInBytes() {
        return bytesIn;
    }
    @Override
    public long getInMsgs() {
        return messagesIn;
    }
    @Override
    public long getOutMsgs() {
        return messagesOut;
    }
    @Override
    public long getReconnects() {
        return reconnects;
    }
    @Override
    public long getDroppedCount() {
        return droppedMessages;
    }
}
