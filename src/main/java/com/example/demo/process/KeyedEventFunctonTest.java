package com.example.demo.process;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.Nullable;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/12 18:36
 */
public class KeyedEventFunctonTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamSource = env.addSource(new SourceFun());
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = streamSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Event event) {
                        return event.timestamp;
                    }
                }
        );
        eventSingleOutputStreamOperator.keyBy(data -> data.username).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {

                long l = context.timestamp();
                System.out.println("打印l:" + l);
                collector.collect(context.getCurrentKey() + "开始修仙，开始修仙时间：" + new Timestamp(l) + "watermark:" + context.timerService().currentWatermark());
                context.timerService().registerEventTimeTimer(l + 5000);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey() + "神功大成，：" + new Timestamp(timestamp));
            }
        }).print();

        env.execute();


    }
}
