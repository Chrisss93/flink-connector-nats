package com.github.chrisss93.connector.nats.testutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;

import java.util.UUID;

public class SinkCollector<T> {
    private final CollectSinkOperator<T> operator;
    private final TypeSerializer<T> serializer;
    private final String accumulatorName;
    private final CheckpointConfig checkpointConfig;

    public SinkCollector(
        CollectSinkOperator<T> operator,
        TypeSerializer<T> serializer,
        String accumulatorName,
        CheckpointConfig checkpointConfig) {
        this.operator = operator;
        this.serializer = serializer;
        this.accumulatorName = accumulatorName;
        this.checkpointConfig = checkpointConfig;
    }

    public CollectResultIterator<T> build(JobClient jobClient) {
        CollectResultIterator<T> iterator = new CollectResultIterator<>(
            operator.getOperatorIdFuture(),
            serializer,
            accumulatorName,
            checkpointConfig
        );
        iterator.setJobClient(jobClient);
        return iterator;
    }


    public static <T> SinkCollector<T> apply(DataStream<T> stream) {
        TypeSerializer<T> serializer = stream.getType().createSerializer(stream.getExecutionConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory = new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());

        return new SinkCollector<>(
            operator,
            serializer,
            accumulatorName,
            stream.getExecutionEnvironment().getCheckpointConfig());
    }
}