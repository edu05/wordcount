package edu.streaming;

import edu.api.TimestampedWord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimestampedWordCountingFunction extends KeyedProcessFunction<String, TimestampedWord, TimestampedWordCount> implements CheckpointedFunction {


    ValueState<TimestampedWordCount> state;

//    @Override
//    public void open(Configuration parameters) throws Exception {
//        state = getRuntimeContext().getState(new ValueStateDescriptor<>("timestampedWordCountingFunctionState", TimestampedWordCount.class));
//    }

    @Override
    public void processElement(TimestampedWord timestampedWord, Context ctx, Collector<TimestampedWordCount> outputStream) throws Exception {
        TimestampedWordCount timestampedWordCount = state.value();
        if (timestampedWordCount == null) {
            timestampedWordCount = new TimestampedWordCount(timestampedWord, 0);
        }

        TimestampedWordCount updatedTimestampedWordCount = new TimestampedWordCount(timestampedWord, timestampedWordCount.getCount() + 1);
        state.update(updatedTimestampedWordCount);
        outputStream.collect(updatedTimestampedWordCount);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ValueStateDescriptor<TimestampedWordCount> keyedCnt = new ValueStateDescriptor<>("keyedCnt", TimestampedWordCount.class);

        state = context.getKeyedStateStore().getState(keyedCnt);
    }
}