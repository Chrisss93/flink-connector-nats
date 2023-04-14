# Apache Flink NATS Connector

This repository provides a Flink Source and Sink Connector for a JetStream-enabled NATS cluster. These connectors use the new [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) Source API and [FLIP-143](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API) Unified Sink API.

It tries to follow a similar structure to the [flink-connector-kafka](https://github.com/apache/flink-connector-kafka) and [flink-connector-pulsar](https://github.com/apache/flink-connector-pulsar) projects.

JetStream enables source replay, but NATS still lacks transactions. Therefore, this connector is only at-least-once. In order for the connector to be *effectively once*, one can configure the NATS sink stream to perform infinite (time-unbounded) message deduplication on the server-side. This *should* make the sink idempotent under the right configuration. More about this [here](https://nats.io/blog/new-per-subject-discard-policy/).

## Source

The source connector is designed to consume from a single NATS stream by way of 1 or more NATS consumers. Specifying multiple consumers with non-overlapping subject-filters allows different portions of the stream to be read and processed in parallel (*somewhat* like a Kafka/Pulsar topic with multiple partitions).  The connector will create the NATS consumers and will handle their entire lifecycle.

If a "default" consumer is configured, the connector will automatically create a NATS consumer from the default consumer settings for each subject-filter on the NATS stream to ensure that every message in the stream is being delivered to the connector and the user doesn't need to fiddle around with subject-filters.

When the connector is configured with multiple explicit consumers rather than a default consumer, each of the consumers' subject-filters must be non-overlapping  to ensure that the same message in a NATS stream is not read by more than 1 NATS consumer.

### Usage

```java
import com.github.chrisss93.connector.nats.source.JetStreamSource;
import com.github.chrisss93.connector.nats.source.reader.deserializer.StringDeserializer;

JetStreamSource<String> source = JetStreamSource.<String>builder()
    .setServerURL("nats://...")
    .setStream("foo")
    .setDefaultConsumerConfiguration("my-flink-job")
    .setDeserializationSchema(new StringDeserializer())
    .build();

DataStream<String> myStream = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "NATS Source"
);
```

### Bounded mode

The user may specify a simple rule by which the connector will stop consuming a NATS stream after some condition specified in the `connector.nats.source.enumerator.offsets.StopRule` interface. This sets the connector to Bounded-mode. **These stopping-rules are scoped to the entire NATS stream, not the source splits (NATS consumers)**. So in the situation where the connector is consuming a NATS stream via multiple splits, the first split which reaches the stopping rule will trigger all other splits to gracefully stop consuming from their respective NATS consumers. When there is greater parallelism than splits, the following steps are taken to achieve this:

1. The `SourceReader` for the stopping split sends a custom `org.apache.flink.api.connector.source.SourceEvent` to the `SplitEnumerator` (source coordinator)
2. The enumerator broadcasts that same source-event to the other source-readers.
3. The other source-readers receive the source-event and use their `SplitFetcherManager` to send a special `org.apache.flink.connector.base.source.reader.splitreader.SplitChanges` to the source-readers' fetchers (`SplitReader`). This instructs the split-reader to mark all its splits as finished on the next fetch call.

The motivation behind this extra communication between source components is to ensure that an idle split does not keep a bounded source open for longer than is necessary. Since the stopping-rule is globally scoped, this short-circuits the other splits to close immediately instead of staying running in the case where their NATS consumers are not sending new messages.


## Sink

### Usage

```java
import com.github.chrisss93.connector.nats.sink.JetStreamSink;
import com.github.chrisss93.connector.nats.sink.writer.serializer.StringSerializer;
import java.util.Properties;

import static io.nats.client.Options.PROP_URL;

Properties props = new Properties();
props.setProperty(Options.PROP_URL, "nats://...");

StringSerializer serializer = (element) -> "bar";

DataStream<Integer> ds;

ds
    .map(String::valueOf)
    .sinkTo(new JetStreamSink(props, serializer))
    .uid("NATS Sink")
```

The example above will try to send data to the subject: `bar`. If the NATS server doesn't have any stream capturing that subject in its subject-filter (or any active consumers on that subject), the messages will be rejected and the job will fail.

## TODO

* ~~Add Bounded mode for Source connector~~
* ~~Add a canonical builder~~
* Performance benchmarks
* Telemetry
* ~~Source unit-tests~~
* ~~Sink unit-tests~~
* Table API?
* ~~E2E testing~~
* Implement ContainerRuntime for unit-tests to avoid external binary dependency
* Publishing artifacts (against multiple Flink versions)
* ~~Simplify `JetStreamConsumerSplit` to just be the pending acks, stream name and consumer name. No need to carry the entire un-serializable NATS ConsumerInfo~~
* ~~Somehow test that the connector does not rely on NATS for fault tolerance. Instead, handle progress recovery seamlessly as Kafka/Pulsar connectors do through checkpoints by default.~~
* ~~When library compiles against Flink 1.17+, implement the additional method from FLIP-217 for split watermark alignment. This is of particular importance to this connector because there are even fewer guarantees than with Kafka that the split-readers (NATS consumers) will be receiving data evenly from the underlying stream.~~
* Potentially remove support for NATS Ack-All and only support a cumulative acknowledgement model. This would allow us to remove an abstraction and simplify the checkpointing code a bit. This would probably mean we can remove the single vs. double ack code-path as well (should really be using double-ack at all times).
* Allow the split-reader to receive messages from NATS using a Dispatcher instead of one or more pull-subscriptions. The problem here is that the jnats dispatcher construct was designed for push-subscriptions, not pull-subscriptions. So we might need to implement a lot of custom code for this.
* NATS streams (and their splits under this connector) are much more mutable than Kafka topic-partitions. Test to see what happens if the underlying NATS stream changes its subject-filters? If the subject-space is now smaller and some of our split-readers have been initiated with NATS consumers whose subject-filter that is now wider than its stream subject-filter, does the client throw/warn? Or do we need to monitor this to be able to clean up resources for these newly dead splits?
  * Furthermore, if the source connector is configured to dynamically create splits based on the subject filters in the stream and new subject-filters are simply *added* to the stream, should we do the dynamic-split discovery like kafka/pulsar and assign a new split?
* Subject-filter is not a "natural" partitioning for a NATS stream. There are no guarantees of even distribution of stream data across subjects. In this case it should not be unexpected for one single split to produce more data than another. Split skewness is (in general) not real for these sorts of splits. We should probably turn off split-aware watermark generation or make it the configurable default and go back to global source watermarks (ReaderOutput). This is difficult with the current implementation since it extends `SourceReaderBase`. I really don't want to have to reimplement most of that class but for one small change...
* BUG: LastAck mode doesn't seem to clear old successful acks from the last checkpoint.