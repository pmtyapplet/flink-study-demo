package com.example.demo.state;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeriodicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new SourceFun())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        streamOperator.print("input");
        streamOperator.keyBy(data -> data.username)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    //定义状态，保存当前统计值
                    ValueState<Long> valueState;

                    ValueState<Long> valueStateTimer;


                    //当前生命周期函数
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));
                        valueStateTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
                        super.open(parameters);
                    }

                    //当前数据处理函数
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {

                        // 每来一条数据 就更新对应的count的值
                        Long count = valueState.value();
                        valueState.update(count == null ? 1 : count + 1);

                        //首次进入的数据需要设置一个计时器 ，如果不再触发器触发后就创建一个计时器的话，那么如果没有新的数据推送的情况下，则一直不回有新的数据推送
                        // 没有注册过的话 注册定时器
                        if (valueStateTimer.value() == null) {
                            context.timerService().registerEventTimeTimer(event.timestamp + 10 * 1000L);
                            valueStateTimer.update(event.timestamp + 10 * 1000L);
                        }

                    }

                    //定时器触发器
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        //定时器出发 输出一次信息
                        out.collect(ctx.getCurrentKey() + "pv: " + valueState.value());
                        valueStateTimer.clear();
                        //因为是周期性的统计数据， 所以需要在计时器销毁时立刻创建一个新的计时器，以保证 10秒的一次输出
                        ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
                        valueStateTimer.update(timestamp + 10 * 1000L);
                    }
                }).print();

        env.execute();

    }
}
