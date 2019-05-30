package edu.stupid;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducer {

    private static final String PRODUCER_TOPIC = "my-first-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static Map<String, Object> KAFKA_PRODUCER_CONFIG = new HashMap() {{
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);


    }};

    private static KafkaTemplate<String, String> KAFKA_PRODUCER = new KafkaTemplate(new DefaultKafkaProducerFactory(KAFKA_PRODUCER_CONFIG));

    public static void send(String word) {
        KAFKA_PRODUCER.send(PRODUCER_TOPIC, word, word);
    }
}
