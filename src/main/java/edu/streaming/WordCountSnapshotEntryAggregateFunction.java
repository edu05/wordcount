package edu.streaming;

import edu.api.WordCountsSnapshot;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

public class WordCountSnapshotEntryAggregateFunction implements AggregateFunction<WordCountSnapshotEntry, WordCountsSnapshot, WordCountsSnapshot> {

    @Override
    public WordCountsSnapshot createAccumulator() {
        return new WordCountsSnapshot(-1, new HashMap<>());
    }

    @Override
    public WordCountsSnapshot add(WordCountSnapshotEntry wordCountSnapshotEntry, WordCountsSnapshot wordCountsSnapshot) {
        Map<String, Integer> newWordCounts = wordCountsSnapshot.getWordCounts();
        newWordCounts.put(wordCountSnapshotEntry.getWord(), wordCountSnapshotEntry.getCount());
        return new WordCountsSnapshot(wordCountSnapshotEntry.getTimestamp(), newWordCounts);
    }

    @Override
    public WordCountsSnapshot getResult(WordCountsSnapshot wordCountsSnapshot) {
        return wordCountsSnapshot;
    }

    @Override
    public WordCountsSnapshot merge(WordCountsSnapshot firstAccumulator, WordCountsSnapshot secondAccumulator) {

        long snapshotTime = firstAccumulator.getSnapshotTime() != -1 ? firstAccumulator.getSnapshotTime() : secondAccumulator.getSnapshotTime();
        Map<String, Integer> mergedWordCounts = new HashMap<>();
        mergedWordCounts.putAll(firstAccumulator.getWordCounts());
        mergedWordCounts.putAll(secondAccumulator.getWordCounts());
        return new WordCountsSnapshot(snapshotTime, mergedWordCounts);
    }
}