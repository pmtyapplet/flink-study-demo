package com.example.demo.state;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SinkStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //     env.setParallelism(1);
        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new SourceFun())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        streamOperator.print("input");
        streamOperator.addSink(new BufferingSink(10));
        env.execute();

    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {

        //定义当前累的属性，批量
        private final int threshold;


        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferElements = new ArrayList<>();
        }

        //
        private final List<Event> bufferElements;
        //定义一个蒜子状态
        private ListState<Event> checkpointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferElements.add(value);
            //判断如果达到值  就批量写入
            if (bufferElements.size() == threshold) {
                //用打印模拟写入外部系统
                for (Event bufferElement : bufferElements) {
                    System.out.println(bufferElement);
                }
                System.out.println("数据输出成功～");
                bufferElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            //清空状态
            checkpointedState.clear();

            //对状态进行持久化,复制缓存的列表到列表状态
            for (Event bufferElement : bufferElements) {
                checkpointedState.add(bufferElement);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义状态，该状态是蒜子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("bufferd-elements", Event.class);
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);
            //如果从故障回复，则需要将listState中的数据复制到列表中
            if (context.isRestored()) {
                for (Event bufferElement : checkpointedState.get()) {
                    bufferElements.add(bufferElement);
                }

            }
        }
    }
}
