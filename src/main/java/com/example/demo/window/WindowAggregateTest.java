package com.example.demo.window;

import com.example.demo.dot.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/7/30 19:17
 */
public class WindowAggregateTest {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        //设置一个圆
//        env.getConfig().setAutoWatermarkInterval(100);
//        AvgPv avgPv = new AvgPv();
//        SingleOutputStreamOperator<Event> stream = env.fromElements(
//                        new Event("Mary", "", 1000L),
//                        new Event("Bob", "", 2000L),
//                        new Event("ALice", "", 3000L),
//                        new Event("Bob", "", 3300L),
//                        new Event("ALice", "", 3200L),
//                        new Event("Bob", "", 3400L),
//                        new Event("Bob", "", 3800L),
//                        new Event("Bob", "", 4200L)
//                )
//                //无序的水位线,需定义一个延迟时间,针对数据内的时间戳
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(2L)) {
//
//                    @Override
//                    public long extractTimestamp(Event event) {
//                        return event.timestamp;
//                    }
//                });
//        stream.keyBy(Event::getUsername).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).apply(
//                new WindowFunction<Event, String, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<Event> iterable, Collector<String> collector) throws Exception {
//
//                        long start = timeWindow.getStart();
//
//                        long end = timeWindow.getEnd();
//
//                        collector.collect("");
//                    }
//                }
//        ).print();
//
//        stream.keyBy(data -> true).timeWindow(Time.seconds(10), Time.seconds(2)).reduce(new ReduceFunction<Event>() {
//            @Override
//            public Event reduce(Event event, Event t1) throws Exception {
//                return null;
//            }
//        }).print();
//    }
//
//
//    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
//
//
//        @Override
//        public Tuple2<Long, HashSet<String>> createAccumulator() {
//            return Tuple2.of(0L, new HashSet<>());
//        }
//
//        @Override
//        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
//            //每来一条数据，pv个数加1 将user放入set
//            longHashSetTuple2.f1.add(event.username);
//            return Tuple2.of(longHashSetTuple2.f0 + 1, longHashSetTuple2.f1);
//        }
//
//        @Override
//        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
//            //窗口触发时 输出pv和uv的值
//            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
//        }
//
//        @Override
//        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
//            return null;
//        }
//    }
}
