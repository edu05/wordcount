package edu.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.api.TimestampedWord;
import edu.api.WordCountsSnapshot;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
import java.util.UUID;

import static edu.kafkaharness.KafkaProducer.PRODUCER_TOPIC;
import static org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime;

public class AppWithKafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .setMaxParallelism(1)
                .setStateBackend(new RocksDBStateBackend("file:///C/Users/eduardo/dev/backends"));

        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

        env.setStreamTimeCharacteristic(ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", UUID.randomUUID().toString().substring(6));

        ObjectMapper objectMapper = new ObjectMapper();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(PRODUCER_TOPIC, new SimpleStringSchema(), properties);
        SingleOutputStreamOperator<TimestampedWordCount> timestampedWordCountsStream = env.addSource(kafkaConsumer)
                .map(s -> objectMapper.readValue(s, TimestampedWord.class))
                .keyBy(TimestampedWord::getWord)
                .process(new TimestampedWordCountingFunction());


        timestampedWordCountsStream.addSink(new PrintSinkFunction<>());
        SingleOutputStreamOperator<String> aggregate = timestampedWordCountsStream.keyBy(TimestampedWordCount::getWord)
                .process(new WordCountSnapshottingFunction())
                .timeWindowAll(Time.seconds(5))
                .aggregate(new WordCountSnapshotEntryAggregateFunction())
                .map((MapFunction<WordCountsSnapshot, String>) value -> objectMapper.writeValueAsString(value));

        aggregate.addSink(new PrintSinkFunction<>());
        aggregate.addSink(new FlinkKafkaProducer<>("snapshot-topic", new SimpleStringSchema(), properties));

        env.execute();
    }
}