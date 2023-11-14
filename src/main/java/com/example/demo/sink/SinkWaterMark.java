package com.example.demo.sink;

import com.example.demo.dot.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jetbrains.annotations.Nullable;
import org.apache.flink.api.common.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/7/23 15:52
 */
public class SinkWaterMark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置一个圆
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.fromElements(
                        new Event("Mary", "", 1000L),
                        new Event("Bob", "", 2000L),
                        new Event("ALice", "", 3000L),
                        new Event("Bob", "", 3300L),
                        new Event("ALice", "", 3200L),
                        new Event("Bob", "", 3400L),
                        new Event("Bob", "", 3800L),
                        new Event("Bob", "", 4200L)
                )
                //有序的水位线,可以提取为数据内的时间戳 例如
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
                    @Override
                    public long extractAscendingTimestamp(Event event) {
                        return event.timestamp;
                    }
                });


        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator1 = env.fromElements(
                        new Event("Mary", "", 1000L),
                        new Event("Bob", "", 2000L),
                        new Event("ALice", "", 3000L),
                        new Event("Bob", "", 3300L),
                        new Event("ALice", "", 3200L),
                        new Event("Bob", "", 3400L),
                        new Event("Bob", "", 3800L),
                        new Event("Bob", "", 4200L)
                )
                //无序的水位线,需定义一个延迟时间,针对数据内的时间戳
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(2L)) {

                    @Override
                    public long extractTimestamp(Event event) {
                        return event.timestamp;
                    }
                });


        IngestionTimeExtractor<Event> objectIngestionTimeExtractor = new IngestionTimeExtractor<>();

        env.fromElements(
                        new Event("Mary", "", 1000L),
                        new Event("Bob", "", 2000L),
                        new Event("ALice", "", 3000L),
                        new Event("Bob", "", 3300L),
                        new Event("ALice", "", 3200L),
                        new Event("Bob", "", 3400L),
                        new Event("Bob", "", 3800L),
                        new Event("Bob", "", 4200L)
                )
                //指定一个 水位线,并将提取时间戳,可以自己提取对应的时间戳

                //定义了水位线可以与开窗函数配合使用

                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Event>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return objectIngestionTimeExtractor.getCurrentWatermark();
                    }

                    @Override
                    public long extractTimestamp(Event event, long l) {

                        return 0;
                    }
                }).keyBy(event -> event.username).timeWindow(Time.milliseconds(100)).reduce(new ReduceFunction<Event>() {
                    @Override
                    public Event reduce(Event event, Event t1) throws Exception {

                        return null;
                    }
                });


        //使用帮我们实现好的 有界无序
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "", 1000L),
                new Event("Bob", "", 2000L),
                new Event("ALice", "", 3000L),
                new Event("Bob", "", 3300L),
                new Event("ALice", "", 3200L),
                new Event("Bob", "", 3400L),
                new Event("Bob", "", 3800L),
                new Event("Bob", "", 4200L)
        );
        eventDataStreamSource.assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<Event>) WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        );

    }
}
