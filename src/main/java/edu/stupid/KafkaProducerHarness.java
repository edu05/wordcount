package edu.stupid;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class KafkaProducerHarness {

    private static final String EDU = "edu";
    private static final String BARBARA = "barbara";
    private static final String MARIO = "mario";
    private static final String LUIGI = "luigi";
    private static final String PEACH = "peach";
    private static final List<String> WORDS = Arrays.asList(EDU, BARBARA, MARIO, LUIGI, PEACH);
    public static final Random RANDOM = new Random();

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            KafkaProducer.send(WORDS.get(RANDOM.nextInt(WORDS.size())));
        }
    }
}
