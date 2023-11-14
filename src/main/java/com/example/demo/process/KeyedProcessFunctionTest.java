package com.example.demo.process;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;


import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/11 18:22
 */
public class KeyedProcessFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamSource = env.addSource(new SourceFun());
        streamSource.keyBy(data -> data.username).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {

                long l = context.timerService().currentProcessingTime();
                collector.collect(context.getCurrentKey() + "开始修仙，开始修仙时间：" + new Timestamp(l));
                context.timerService().registerProcessingTimeTimer(l + 5000);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey() + "神功大成，：" + new Timestamp(timestamp));
            }
        }).print();

        env.execute();


    }

}
