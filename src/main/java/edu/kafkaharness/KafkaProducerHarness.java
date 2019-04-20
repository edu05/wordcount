package edu.kafkaharness;

import edu.api.TimestampedWord;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class KafkaProducerHarness {

    public static final String RED_EDU = "\u001B[31medu\u001B[0m";
    public static final String GREEN_BARBARA = "\u001B[32mbarbera\u001B[0m";
    public static final String BLUE_MARIO = "\u001B[34mmario\u001B[0m";
    public static final String YELLOW_LUIGI = "\u001B[33mluigi\u001B[0m";
    public static final String PURPLE_PEACH = "\u001B[35mpeach\u001B[0m";
    private static final List<String> WORDS = Arrays.asList(RED_EDU, GREEN_BARBARA, BLUE_MARIO, YELLOW_LUIGI, PURPLE_PEACH);
    public static final Random RANDOM = new Random();

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            double p = RANDOM.nextDouble();
            if (p < 0.6) {
                Thread.sleep(1000);
            } else if (p < 0.8) {
                Thread.sleep(500);
            } else if (p < 0.97) {
                Thread.sleep(2000);
            } else {
                IntStream.range(0, 4).forEach(i -> sendNewMessage());
            }
            sendNewMessage();
        }

    }

    private static void sendNewMessage() {
        TimestampedWord timestampedWord = new TimestampedWord(WORDS.get(RANDOM.nextInt(WORDS.size())), ZonedDateTime.now(ZoneOffset.UTC).toInstant().toEpochMilli());
        KafkaProducer.send(timestampedWord);
        System.out.println("sending: " + timestampedWord);
    }
}
