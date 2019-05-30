package edu.api;

import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;

public class WordCountsSnapshot {

    private long snapshotTime;
    private Map<String, Integer> wordCounts;

    private WordCountsSnapshot(){
    }

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

    public void setSnapshotTime(long snapshotTime) {
        this.snapshotTime = snapshotTime;
    }

    public void setWordCounts(Map<String, Integer> wordCounts) {
        this.wordCounts = wordCounts;
    }

    @Override
    public String toString() {
        return ofInstant(ofEpochMilli(snapshotTime), UTC).toLocalTime() + " -> " + wordCounts;
    }
}