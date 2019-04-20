package edu.streaming;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;

public class WordCountSnapshottingFunction extends KeyedProcessFunction<String, TimestampedWordCount, WordCountSnapshotEntry> {

    private static final int TIME_PER_SNAPSHOT = 60 * 1000;
    private static final int ALLOWED_DELAY = 5 * 1000;
    private transient MapState<Long, TimestampedWordCount> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(new MapStateDescriptor<>("timestampedWordCountingFunctionState", Long.class, TimestampedWordCount.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WordCountSnapshotEntry> out) throws Exception {
        long snapshotTimeBeingReported = getPreviousSnapshotTime(timestamp);
        TimestampedWordCount timestampedWordCount = state.get(snapshotTimeBeingReported);
        state.remove(snapshotTimeBeingReported);
        out.collect(new WordCountSnapshotEntry(timestampedWordCount.getTimestampedWord().getWord(), timestampedWordCount.getCount(), snapshotTimeBeingReported));

        if (timestampedWordCount.getCount() != 0) {
            ctx.timerService().registerProcessingTimeTimer(getSnapshotYouBelongTo(timestamp) + ALLOWED_DELAY);
        }
    }

    @Override
    public void processElement(TimestampedWordCount timestampedWordCount, Context ctx, Collector<WordCountSnapshotEntry> out) throws Exception {
        long snapshotTime = getSnapshotYouBelongTo(timestampedWordCount.getTimestampedWord().getTimestamp());
        state.put(snapshotTime, timestampedWordCount);
        long runTime = snapshotTime + ALLOWED_DELAY;
        ctx.timerService().registerProcessingTimeTimer(runTime);
    }

    private static long getSnapshotYouBelongTo(long timestamp) {
        return asZonedDateTime(timestamp).truncatedTo(ChronoUnit.MINUTES).plusMinutes(1).toInstant().toEpochMilli();
    }

    private static long getPreviousSnapshotTime(long timestamp) {
        return getSnapshotYouBelongTo(timestamp) - TIME_PER_SNAPSHOT;
    }

    private static ZonedDateTime asZonedDateTime(long timestamp) {
        return ofInstant(ofEpochMilli(timestamp), UTC);
    }
}