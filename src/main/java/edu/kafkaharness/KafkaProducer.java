package edu.kafkaharness;

import edu.api.TimestampedWord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducer {

    public static final String PRODUCER_TOPIC = "my-first-topic";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static Map<String, Object> KAFKA_PRODUCER_CONFIG = new HashMap() {{
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);


    }};

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static KafkaTemplate<String, TimestampedWord> KAFKA_PRODUCER = new KafkaTemplate(new DefaultKafkaProducerFactory(KAFKA_PRODUCER_CONFIG));

    public static void send(TimestampedWord timestampedWord) {
        try {
            KAFKA_PRODUCER.send(PRODUCER_TOPIC, OBJECT_MAPPER.writeValueAsString(timestampedWord.getWord()), timestampedWord);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
