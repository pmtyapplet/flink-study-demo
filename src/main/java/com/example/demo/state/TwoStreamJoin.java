package com.example.demo.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TwoStreamJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator1 = env.fromElements(
                Tuple3.of("a", "steam-1", 1000L),
                Tuple3.of("b", "steam-1", 1000L),
                Tuple3.of("a", "steam-1", 2000L),
                Tuple3.of("b", "steam-1", 2000L),
                Tuple3.of("a", "steam-1", 3000L),
                Tuple3.of("b", "steam-1", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> value, long l) {
                        return value.f2;
                    }
                }
        ));


        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator2 = env.fromElements(
                Tuple3.of("a", "steam-2", 3000L),
                Tuple3.of("b", "steam-2", 4000L),
                Tuple3.of("a", "steam-2", 5000L),
                Tuple3.of("b", "steam-2", 6000L),
                Tuple3.of("a", "steam-2", 7000L),
                Tuple3.of("b", "steam-2", 8000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> value, long l) {
                        return value.f2;
                    }
                }
        ));


        streamOperator1.connect(streamOperator2).keyBy(data1 -> data1.f0, data2 -> data2.f0).process(
                new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    //定义状态
                    ListState<Tuple3<String, String, Long>> stream1ListState;
                    ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream-1", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                        stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream-2", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {

                        //当前为 第一条流的数据
                        for (Tuple3<String, String, Long> stream2 : stream2ListState.get()) {
                            collector.collect("left" + left + " = > " + stream2);
                        }
                        stream1ListState.add(left);
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                        for (Tuple3<String, String, Long> stream1 : stream1ListState.get()) {
                            collector.collect("right" + right + " = > " + stream1);
                        }
                        stream2ListState.add(right);
                    }
                }
        ).print();

        env.execute();
    }
}
