package com.example.demo.state;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AggStateExmaple {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> streamOperator = executionEnvironment.addSource(new SourceFun())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        streamOperator.print("input");
        streamOperator.keyBy(data -> data.url)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    AggregatingState<Long, Long> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Long, Long, Long>(
                                "agg-state",
                                new AggregateFunction<Long, Long, Long>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(Long aLong, Long aLong2) {
                                        return aLong + 1;
                                    }

                                    @Override
                                    public Long getResult(Long aLong) {
                                        return aLong;
                                    }

                                    @Override
                                    public Long merge(Long aLong, Long acc1) {
                                        return null;
                                    }
                                }
                                ,
                                Long.class
                        ));

                    }

                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> out) throws Exception {


                    }
                }).print();
        executionEnvironment.execute();
    }
}
