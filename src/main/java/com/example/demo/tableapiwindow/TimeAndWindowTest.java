package com.example.demo.tableapiwindow;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import lombok.val;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/10/28 16:59
 */
public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<Event> stream = env.addSource(new SourceFun())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );

        Table table = tableEnv.fromDataStream(stream, $("username"), $("url"), $("timestamp").rowtime());
        table.printSchema();

        Table select = table.window(Tumble.over(lit(10).second()).on($("timestamp")).as("ts"))
                .groupBy($("username"), $("ts")).select(
                        $("username"),
                        $("ts").start().as("ts"),
                        $("url").count().as("cnt"));

        Table select1 = table.window(Slide.over(lit(10).second()).every(lit(5).second()).on($("timestamp")).as("ts"))
                .groupBy($("username"), $("ts")).select(
                        $("username"),
                        $("ts").start().as("ts"),
                        $("url").count().as("cnt"));
        select.printSchema();
        select1.printSchema();
      //  tableEnv.toChangelogStream(select).print("Tumble");
        tableEnv.toChangelogStream(select1).print("Slide");
        env.execute();
    }

}
