package edu.kafkaharness;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {KafkaConsumerConfig.class})
public class KafkaReaderHarness {

    @Test
    public void name() throws InterruptedException {
        Thread.sleep(10000);
    }
}
