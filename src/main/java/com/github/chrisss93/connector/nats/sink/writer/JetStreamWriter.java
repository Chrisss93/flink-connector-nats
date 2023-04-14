package com.github.chrisss93.connector.nats.sink.writer;

import com.github.chrisss93.connector.nats.sink.metrics.JetStreamSinkWriterMetrics;
import com.github.chrisss93.connector.nats.sink.writer.serializer.MessageSerializationSchema;
import io.nats.client.*;
import io.nats.client.api.PublishAck;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/** We need to explicitly block publishing beyond a maximum of pending outgoing messages because, counter to
 * documentation, <a href="https://github.com/nats-io/nats.java/issues/876">the NATS client doesn't block for us,
 * but instead throws if we reach its internal limit.</a>
 */
public class JetStreamWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JetStreamWriter.class);
    private static final long FLUSH_TIMEOUT_MS = 10000L;
    private static final long METRIC_UPDATE_INTERVAL_MS = 500L;

    private final Connection connection;
    private final JetStream js;
    private final MessageSerializationSchema<T> serializationSchema;
    private final MailboxExecutor mailboxExecutor;
    private final ProcessingTimeService timeService;
    private final int maxPendingMessages;
    private final JetStreamSinkWriterMetrics writerMetrics;
    public AtomicLong pendingMessages = new AtomicLong(0L);
    private boolean closed = false;
    private long lastSync = System.currentTimeMillis();

    public JetStreamWriter(
        Properties connectProps,
        MessageSerializationSchema<T> serializationSchema,
        Sink.InitContext initContext) {

        Options connectOptions = new Options.Builder(connectProps).turnOnAdvancedStats().build();
        this.maxPendingMessages = connectOptions.getMaxMessagesInOutgoingQueue();
        this.serializationSchema = serializationSchema;
        this.mailboxExecutor = initContext.getMailboxExecutor();
        this.timeService = initContext.getProcessingTimeService();

        try {
            connection = Nats.connect(connectOptions);
            js = connection.jetStream();
            this.serializationSchema.open(initContext.asSerializationSchemaInitializationContext());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Can't connect to NATS", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlinkRuntimeException("Can't connect to NATS", e);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        writerMetrics = new JetStreamSinkWriterMetrics(connection, initContext.metricGroup());
        registerMetricSync();
    }

    @Override
    public void write(T element, Context context) throws InterruptedException {
        Message msg = serializationSchema.makeMessage(element, context.timestamp());
        while (pendingMessages.get() >= maxPendingMessages) {
            LOG.warn("Too many pending messages to write. Blocking future writes until backlog clears.");
            mailboxExecutor.yield();
        }
        publishWithCallback(msg);
    }

    private void publishWithCallback(Message msg) {
        CompletableFuture<PublishAck> future = js.publishAsync(msg);
        mailboxExecutor.execute(() -> future.whenComplete(
            (ack, e) -> {
                if (e != null) {
                    LOG.error("JetStream did not acknowledge message publication: {}.", e.getMessage());
                } else if (ack.isDuplicate()) {
                    LOG.warn(
                        "NATS has marked the message at stream {} (#{}) as a duplicate.",
                        ack.getStream(), ack.getSeqno()
                    );
                }
                pendingMessages.decrementAndGet();
                writerMetrics.updateAcks(ack);
            }).get(),
            "Async publish handler"
        );
        pendingMessages.incrementAndGet();
    }

    @Override
    public void flush(boolean endOfInput) throws InterruptedException {
        if (pendingMessages.get() < 1) {
            return;
        }
        LOG.info("Flushing {} pending messages to NATS.", pendingMessages.get());
        try {
            connection.flush(Duration.ofMillis(FLUSH_TIMEOUT_MS));
        } catch (TimeoutException e) {
            throw new FlinkRuntimeException("Took too long to write data to NATS", e);
        }

        while (pendingMessages.get() > 0) {
            mailboxExecutor.yield();
        }
    }

    private void registerMetricSync() {
        timeService.registerTimer(
            lastSync + METRIC_UPDATE_INTERVAL_MS,
            (time) -> {
                if (!closed) {
                    writerMetrics.updateMetrics();
                    writerMetrics.updateInFlightRequests(pendingMessages);
                    lastSync = time;
                    registerMetricSync();
                    // TODO: Sample and set the current-send-time between publish and acknowledgement
                }
            });
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (connection != null) {
            connection.flush(Duration.ofMillis(FLUSH_TIMEOUT_MS));
            connection.close();
        }
    }

    @VisibleForTesting
    long getPendingMessages() {
        return this.pendingMessages.get();
    }
}