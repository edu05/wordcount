package edu.api;


import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;

public class TimestampedWord {
    private String word;
    private long timestamp;

    private TimestampedWord() {
    }

    public TimestampedWord(String word, long timestamp) {
        this.word = word;
        this.timestamp = timestamp;
    }

    public String getWord() {
        return word;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("%-18s@ %s", word, ofInstant(ofEpochMilli(timestamp), UTC).toLocalTime());
    }
}
