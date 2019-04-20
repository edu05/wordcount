package edu.streaming;

import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;

public class WordCountsSnapshot {

    private final long snapshotTime;
    private final Map<String, Integer> wordCounts;

    public WordCountsSnapshot(long snapshotTime, Map<String, Integer> wordCounts) {
        this.snapshotTime = snapshotTime;
        this.wordCounts = wordCounts;
    }

    public long getSnapshotTime() {
        return snapshotTime;
    }

    public Map<String, Integer> getWordCounts() {
        return wordCounts;
    }

    @Override
    public String toString() {
        return ofInstant(ofEpochMilli(snapshotTime), UTC).toLocalTime() + " -> " + wordCounts;
    }
}