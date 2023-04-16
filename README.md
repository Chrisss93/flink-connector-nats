# Apache Flink NATS Connector

This repository provides a Flink Source and Sink Connector for a JetStream-enabled NATS cluster. These connectors use the new [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) Source API and [FLIP-143](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API) Unified Sink API for bounded and unbounded streams.

It tries to follow a similar structure to the [flink-connector-kafka](https://github.com/apache/flink-connector-kafka) and [flink-connector-pulsar](https://github.com/apache/flink-connector-pulsar) projects.

JetStream enables source replay, but NATS still lacks transactions. Therefore, this connector is only at-least-once. In order for the connector to be *effectively once*, one can configure the NATS sink stream to perform infinite (time-unbounded) message deduplication on the server-side. This *should* make the sink idempotent under the right configuration. More about this [here](https://nats.io/blog/new-per-subject-discard-policy/).

### Features

- [x] Bounded mode for Source connector
- [x] Failure recovery via Flink checkpoints with no other external dependencies
- [x] Unit+e2e tests for source+sink connector
- [x] Flink metrics and telemetry for source+sink connector
- [x] Periodic, dynamic split discovery for source connector
- [ ] Table API
- [ ] Publish artifacts

## Source

The source connector is designed to consume from a single NATS stream by way of 1 or more NATS consumers. Specifying multiple consumers with non-overlapping subject-filters allows different portions of the stream to be read and processed in parallel (*somewhat* like a Kafka/Pulsar topic with multiple partitions).  The connector will create the NATS consumers and will handle their entire lifecycle.

If a "default" consumer is configured, the connector will automatically create a NATS consumer from the default consumer settings for each subject-filter on the NATS stream to ensure that every message in the stream is being delivered to the connector and the user doesn't need to fiddle around with subject-filters.

When the connector is configured with multiple explicit consumers rather than a default consumer, each of the consumers' subject-filters is validated to make sure they are non-overlapping (or else the same message in a NATS stream would be read multiple times by the source connector).

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

The user may specify a simple rule by which the connector will stop consuming a NATS stream after some condition specified in the `connector.nats.source.enumerator.offsets.StopRule` interface. This sets the connector to Bounded-mode. **These stopping-rules are scoped to the entire NATS stream, not the source splits (NATS consumers)**. So in the situation where the connector is consuming a NATS stream via multiple splits, the first split which reaches the stopping rule will trigger all other splits to gracefully stop consuming from their respective NATS consumers. To achieve this, the following steps are performed:

1. The `SourceReader` for the stopping split sends a custom `org.apache.flink.api.connector.source.SourceEvent` to the `SplitEnumerator` (source coordinator)
2. The enumerator broadcasts that same source-event to the other source-readers.
3. The other source-readers receive the source-event and use their `SplitFetcherManager` to send a special `org.apache.flink.connector.base.source.reader.splitreader.SplitChanges` to the source-readers' fetchers (`SplitReader`). This instructs the split-readers to mark all their splits as finished on the next fetch call.

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

* Potentially remove support for NATS Ack-All and only support a cumulative acknowledgement model. This would allow us to remove an abstraction and simplify the checkpointing code a bit. This would probably mean we can remove the single vs. double ack code-path as well (should really be using double-ack at all times).
* Upgrade NATS client library once [multi-filter consumers](https://github.com/nats-io/nats.java/issues/865) are supported. This will allow us to maintain a single subscription per fetcher (no matter how many splits it is responsible for).
* Should split-manager assign fetchers to split-readers round-robin the same way the enumerator assigns splits to readers? Split-ids will have a lot of common characters, I'm not sure taking its hashcode remainder is the fairest way to assign fetcher threads...
* Subject-filter is not a "natural" partitioning for a NATS stream. There are no guarantees of loosely uniform distribution of the stream's data across subject-filter. In this case it should not be unexpected for one single split to produce more data than another. We should probably turn off split-aware watermark generation or make it the configurable default and go back to global source watermarks (ReaderOutput). This is difficult with the current implementation since it extends `SourceReaderBase`. I really don't want to have to reimplement most of that class but for one small change...
