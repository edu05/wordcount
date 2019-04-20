package edu.streaming;

public class WordCountSnapshotEntry {

    private final String word;
    private final int count;
    private final long timestamp;

    public WordCountSnapshotEntry(String word, int count, long timestamp) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
    }

    public String getWord() {
        return word;
    }

    public int getCount() {
        return count;
    }

    public long getTimestamp() {
        return timestamp;
    }
}