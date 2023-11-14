package com.example.demo.transfer;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/27 18:54
 */
public class CoGroupTest {
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

        streamOperator1.coGroup(streamOperator2)
                .where(data -> data.f0)
                .equalTo(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> iterable, Iterable<Tuple2<String, Long>> iterable1, Collector<String> collector) throws Exception {
                        collector.collect(iterable + "===" + iterable1);
                    }
                }).print();
        env.execute();
    }
}
