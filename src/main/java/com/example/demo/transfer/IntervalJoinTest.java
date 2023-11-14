package com.example.demo.transfer;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/27 18:47
 */
public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        //多流合并， 不同类型的流
        //此次讲不通类型的流 最终输出为同一个类型的流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                return stringLongTuple2.f1;
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator2 = env.fromElements(
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 4000L),
                Tuple2.of("a", 4500L),
                Tuple2.of("b", 5000L)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                return stringLongTuple2.f1;
            }
        }));
        streamOperator1.keyBy(data -> data.f0).intervalJoin(streamOperator2.keyBy(data -> data.f0)).inEventTime().between(Time.seconds(-5), Time.seconds(10)).process(new ProcessJoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
            @Override
            public void processElement(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> stringLongTuple22, ProcessJoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(stringLongTuple2 + "    " + stringLongTuple22);
            }
        }).print();

        env.execute();
    }
}
