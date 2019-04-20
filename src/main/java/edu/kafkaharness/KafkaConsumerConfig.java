package edu.kafkaharness;

import edu.api.TimestampedWord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static edu.kafkaharness.KafkaProducer.BOOTSTRAP_SERVERS;
import static edu.kafkaharness.KafkaProducer.PRODUCER_TOPIC;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public Map<String, Object> consumerConfigs() {
        return new HashMap() {{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    BOOTSTRAP_SERVERS);
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    JsonDeserializer.class);
            put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString().substring(10));
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        }};
    }

    @Bean
    public ConsumerFactory<String, TimestampedWord> consumerFactory() {
        Map<String, Object> configs = consumerConfigs();
        return new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(),  new JsonDeserializer<>(TimestampedWord.class));
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TimestampedWord>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TimestampedWord> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }

    @Bean
    public ConcurrentMessageListenerContainer listenerContainer() {
        ConcurrentMessageListenerContainer<String, TimestampedWord> container = kafkaListenerContainerFactory().createContainer(PRODUCER_TOPIC);
        container.setupMessageListener(new MessageListener<String, TimestampedWord>() {
            @Override
            public void onMessage(ConsumerRecord<String, TimestampedWord> data) {
                System.out.println(data.value());

            }
        });
        return container;
    }
}
