package com.example.demo.tableapiwindow;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/10/30 16:01
 */
public class TopNWindowTableApi {
    public static void main(String[] args)  throws Exception{
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

        Table table = tableEnv.fromDataStream(stream, $("username"), $("url"), $("timestamp").rowtime().as("et"));
        tableEnv.registerTable("event",table);
        table.printSchema();


        //窗口topN，统计一段时间内的前两名的用户
        Table topN = tableEnv.sqlQuery("select username ,cnt, row_num from (select * ,ROW_NUMBER() OVER(   PARTITION BY window_start,window_end  ORDER BY cnt DESC ) AS row_num FROM (select username, count(url) as cnt,window_start,window_end from  TABLE( TUMBLE(TABLE  event, DESCRIPTOR(et), INTERVAL '10' SECOND))     group by username,window_start,window_end) ) where row_num <=2");

        tableEnv.toChangelogStream(topN).print("top:2");
        env.execute();

    }
}
