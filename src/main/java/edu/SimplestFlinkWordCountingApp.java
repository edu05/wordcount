package edu;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime;

public class SimplestFlinkWordCountingApp {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

        env.getConfig().setAutoWatermarkInterval(1000l);

        env.setStreamTimeCharacteristic(ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", UUID.randomUUID().toString().substring(6));

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("my-first-topic", new SimpleStringSchema(), properties);
        env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    private int counter = 0;

                    @Override
                    public long extractAscendingTimestamp(String element) {
                        return counter++;
                    }
                })
                .keyBy(q -> q)
                .process(new CountingFunction())
                .print();
        env.execute();
    }

    private static class CountingFunction extends KeyedProcessFunction<String, String, String> implements CheckpointedFunction {

        private ValueState<Long> qqState;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ValueStateDescriptor<Long> keyedCnt = new ValueStateDescriptor<>("qq", Long.class);
            qqState = context.getKeyedStateStore().getState(keyedCnt);
        }

        @Override
        public void processElement(String word, Context ctx, Collector<String> out) throws Exception {
            Long currentWordCount = qqState.value();
            if (currentWordCount == null) {
                currentWordCount = 0l;
            }

            long updatedWordCount = currentWordCount + 1;
            qqState.update(updatedWordCount);
            out.collect(word + "=" + updatedWordCount);
        }
    }
}
