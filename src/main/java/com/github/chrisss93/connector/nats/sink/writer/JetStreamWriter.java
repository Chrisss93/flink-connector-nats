package com.github.chrisss93.connector.nats.sink.writer;

import com.github.chrisss93.connector.nats.sink.writer.serializer.MessageSerializationSchema;
import io.nats.client.*;
import io.nats.client.api.PublishAck;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
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
    private final Connection connection;
    private final JetStream js;
    private final MessageSerializationSchema<T> serializationSchema;
    private final MailboxExecutor mailboxExecutor;
    private final int maxPendingMessages;
    public AtomicLong pendingMessages = new AtomicLong(0L);

    public JetStreamWriter(
        Properties connectProps,
        MessageSerializationSchema<T> serializationSchema,
        Sink.InitContext initContext) {

        Options connectOptions = new Options.Builder(connectProps).build();
        this.maxPendingMessages = connectOptions.getMaxMessagesInOutgoingQueue();
        this.serializationSchema = serializationSchema;
        this.mailboxExecutor = initContext.getMailboxExecutor();

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
    }

    @Override
    public void write(T element, Context context) throws InterruptedException {
        Message msg = serializationSchema.makeMessage(element, context.timestamp());
        while (pendingMessages.get() >= maxPendingMessages) {
            LOG.info("Too many pending messages");
            mailboxExecutor.yield();
        }
        publishWithCallback(msg);
    }

    private void publishWithCallback(Message msg) {
        CompletableFuture<PublishAck> future = js.publishAsync(msg);
        mailboxExecutor.execute(() -> future.handle(
            (ack, e) -> {
                if (e != null) {
                    throw new FlinkRuntimeException("Failed to send data to NATS at subject: " + msg.getSubject(), e);
                }
                if (ack.isDuplicate()) {
                    LOG.warn("NATS has marked the message at stream-sequence {} as a duplicate.", ack.getSeqno());
                }
                return pendingMessages.decrementAndGet();
            }).get(),
            "Async publish handler"
        );
        pendingMessages.incrementAndGet();
    }

    @Override
    public void flush(boolean endOfInput) throws InterruptedException {
        LOG.info("Flushing {} pending messages to NATS.", pendingMessages.get());
        System.out.printf("Flushing %d pending messages to NATS.%n", pendingMessages.get());
        try {
            connection.flush(Duration.ofMillis(FLUSH_TIMEOUT_MS));
        } catch (TimeoutException e) {
            throw new FlinkRuntimeException("Took too long to write data to NATS", e);
        }

        while (pendingMessages.get() > 0) {
            mailboxExecutor.yield();
//            pendingMessages--;
        }
    }

    @Override
    public void close() throws Exception {
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