package com.example.demo.transfer;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.time.Duration;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/22 19:03
 */
public class SplitSteamTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new SourceFun())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );

        //定义输出标签

        OutputTag<Tuple3<String, String, Long>> aTag = new OutputTag<Tuple3<String, String, Long>>("叶沐晨") {
        };
        OutputTag<Tuple3<String, String, Long>> bTag = new OutputTag<Tuple3<String, String, Long>>("君沫邪") {
        };
        SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                if (event.username.equals("叶沐晨")) {
                    context.output(aTag, Tuple3.of(event.username, event.url, event.timestamp));
                } else if (event.username.equals("君沫邪")) {
                    context.output(bTag, Tuple3.of(event.username, event.url, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });
        processStream.print("else");
        processStream.getSideOutput(aTag).print("叶沐晨");
        processStream.getSideOutput(bTag).print("君沫邪");

        env.execute();
    }
}
