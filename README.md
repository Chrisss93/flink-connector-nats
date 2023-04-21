# Apache Flink NATS Connector

This repository provides a Flink Source and Sink Connector for a JetStream-enabled NATS cluster. These connectors use the new [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) Source API and [FLIP-143](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API) Unified Sink API for bounded and unbounded streams, as well as a Table API implementation.

It tries to follow a similar structure to the [flink-connector-kafka](https://github.com/apache/flink-connector-kafka) and [flink-connector-pulsar](https://github.com/apache/flink-connector-pulsar) projects.

JetStream enables source replay, but NATS still lacks transactions. Therefore, this connector is only at-least-once. In order for the connector to be *effectively once*, one can configure the NATS sink stream to perform infinite (time-unbounded) message deduplication on the server-side. This *should* make the sink idempotent under the right configuration. More about this [here](https://nats.io/blog/new-per-subject-discard-policy/).

### Features

- [x] Bounded mode for Source connector
- [x] Failure recovery via Flink checkpoints with no other external dependencies
- [x] Unit+e2e tests for source+sink connector
- [x] Flink metrics and telemetry for source+sink connector
- [x] Periodic, dynamic split discovery for source connector
- [x] Table API
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

The example above will try to send data to the subject: `bar`. If the NATS server doesn't have any stream capturing that subject in its subject-filter (or any active subscribers), the published messages will be rejected and the job will fail.

## Table/SQL

A Table API factory has been implemented for a `ScanTableSource` and `DynamicTableSink`. `INSERT_ONLY` is the only supported changelog mode. An upsert/delete style logic is possible, but that configuration must be done on the NATS side and for now will remain outside of Flink's Table API purview.

The following metadata fields are available:

| Name         | Type                          | Readable | Writeable |
|--------------|-------------------------------|----------|-----------|
| headers      | MAP<VARCHAR, ARRAY\<VARCHAR>> | &check;  | &check;   |
| nats_subject | VARCHAR                       | &check;  | &check;   |
| stream       | VARCHAR                       | &check;  | &cross;   |
| consumer     | VARCHAR                       | &check;  | &cross;   |
| domain       | VARCHAR                       | &check;  | &cross;   |
| delivered    | BIGINT                        | &check;  | &cross;   |
| streamSeq    | BIGINT                        | &check;  | &cross;   |
| consumerSeq  | BIGINT                        | &check;  | &cross;   |
| pending      | BIGINT                        | &check;  | &cross;   |
| timestamp    | TIMESTAMP_LTZ(9)              | &check;  | &cross;   |
| timezone     | VARCHAR                       | &check;  | &cross;   |

The Table Source supports limit push-down, watermark push-down and a very limited degree of filter push-down.

### Filter push-down

Importantly here, the field-name: `nats_subject` (case-sensitive) is reserved for the corresponding metadata field, and it will be pushed-down if an expression with it is present in the `WHERE` clause of a table query. For now the expression parser remains very simplistic so only basic predicates referencing the unmodified `nats_subject` field and one or more literal values can be pushed down.

For example, the following SQL predicates can be pushed down:
```sql
WHERE subject = 'thing'
```
```sql
WHERE subject LIKE '%.stuff'
```
```sql
WHERE subject IN ('one', 'two')
```

Meanwhile, the following SQL predicates cannot be pushed down and the filtering logic will be applied after the provider has fetched the records from all subjects:

```sql
WHERE subject IN (otherField['validSubjects']) -- referencing another field
```
```sql
WHERE UPPER(subject) == 'SOMETHING' -- transforming the subject
```
```sql
WHERE subject == 'prefix' || 'suffix' -- not a value literal
```

For now, there is a degree of concurrency but also resource-overhead when multiple predicates are pushed down (i.e. `WHERE subject = 'a' OR subject = 'b'` or `WHERE subject IN ('a', 'b')`) since this will create a NATS consumer for each subject filter and read them concurrently. When a push-down filter for multiple subjects is desired, if possible, use a pattern that can capture the desired subjects with the ANSI `LIKE` clause (`%` character will be translated into the NATS full-wildcard).

### Table Sinks

For table sinks (ie. `INSERT` statements), if the `subject` metadata field is not present in the table, a static setting `sink.subject` can be configured to specify the NATS subject that every inserted row will be written to on the NATS server. As with the DataStream Sink API, the  subject (specified either statically with config-options or dynamically with the writeable metadata column) must have a subscriber on the NATS server or else be captured by a NATS stream. Otherwise, the insert statement will fail.

### Usage

```sql
CREATE TABLE foo_input (
  name VARCHAR,
  age INT,
  nats_subject STRING METADATA,
  headers MAP<STRING, ARRAY<STRING>> METADATA,
  `timestamp` TIMESTAMP_LTZ(9) METADATA VIRTUAL
) WITH (
  'connector' = 'nats',
  'io.nats.client.servers' = 'nats://...',
  'stream' = 'foo',
  'format' = 'json'
);

CREATE TABLE bar_output (
  short_name VARCHAR,
  DOB INT,
  nats_subject STRING METADATA,
  headers MAP<STRING, ARRAY<STRING>> METADATA
) WITH (
  'connector' = 'nats',
  'io.nats.client.servers' = 'nats://...',
  'format' = 'json'
);

INSERT INTO bar_output
SELECT
  UPPER(SUBSTRING(CHAR_LENGTH(name) - 3, name)),
  2023 - age,
  'bar.' || SUBSTR(UPPER(name), 0, 1),
  MAP[
    'version', ARRAY['0.1', '0.1-SNAPSHOT'],
    'traceId', headers['traceId']
  ]
FROM foo_input
WHERE nats_subject IN ('a', 'b') AND age >= 18
LIMIT 10;
```


## TODO

* Potentially remove support for NATS Ack-All and only support a cumulative acknowledgement model. This would allow us to remove an abstraction and simplify the checkpointing code a bit. This would probably mean we can remove the single vs. double ack code-path as well (should really be using double-ack at all times).
* Upgrade NATS client library once [multi-filter consumers](https://github.com/nats-io/nats.java/issues/865) are supported. This will allow us to maintain a single subscription per fetcher (no matter how many splits it is responsible for).
* Should split-manager assign fetchers to split-readers round-robin the same way the enumerator assigns splits to readers? Split-ids will have a lot of common characters, I'm not sure taking its hashcode remainder is the fairest way to assign fetcher threads...
* Subject-filter is not a "natural" partitioning for a NATS stream. There are no guarantees of loosely uniform distribution of the stream's data across subject-filter. In this case it should not be unexpected for one single split to produce more data than another. We should probably turn off split-aware watermark generation or make it the configurable default and go back to global source watermarks (ReaderOutput). This is difficult with the current implementation since it extends `SourceReaderBase`. I really don't want to have to reimplement most of that class but for one small change...
* Fix Integration test flakiness, mainly in source ITCases caused by the test scaffolding prematurely tearing down the Flink MiniCluster before the unit test has finished reading the job results. The unbounded test-cases are fine, since we manage the lifecycle of the job (and thus the mini-cluster) directly.
