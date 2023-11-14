package com.example.demo.tableapi;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;


import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/10/8 18:50
 */
public class SimpleTableExaple {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.addSource(new SourceFun()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(
                new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }
        ));

        //创表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        Schema build = Schema.newBuilder().column("username","STRING").build();
        //流转化表
        Table eventTable = tableEnvironment.fromDataStream(eventSingleOutputStreamOperator,build);


        //直接写sql进行转化
        tableEnvironment.createTemporaryView("event",eventTable);

        Table resultTable = tableEnvironment.sqlQuery("select * from  event " );

        DataStream<Row> rowDataStream = tableEnvironment.toDataStream(resultTable);

        rowDataStream.print();

        env.execute();
    }
}
