package com.example.demo.transfer;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/26 15:58
 */
public class BillCheckExxample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = executionEnvironment.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                return stringStringLongTuple3.f2;
            }
        }));
        //第三方支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> merchantStream = executionEnvironment.fromElements(
                Tuple4.of("order-1", "merchant", "success", 3000L),
                Tuple4.of("order-3", "merchant", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple4<String, String, String, Long> stringStringStringLongTuple4, long l) {
                return stringStringStringLongTuple4.f3;
            }
        }));

        //监测同意支付但在两条流中是否匹配，不匹配就报警

        /**
         * 两种写法
         *  1. 先keyby 在链接
         *   stream.keyBy(data->data.f0).connect(stream2.keyBy(data->data.f0));
         *  2. 先链接 在keyby
         *   stream.connect(stream2).keyBy(data->data.f0,data2->data2.f0);
         *
         */
        appStream.connect(merchantStream).keyBy(data -> data.f0, data2 -> data2.f0).process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
            private ValueState<Tuple3<String, String, Long>> appValueState;
            private ValueState<Tuple4<String, String, String, Long>> merchantValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                appValueState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                merchantValueState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("merchant-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
            }

            @Override
            public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                //当前为app事件流,看另外一个流是否来过
                if (merchantValueState.value() != null) {
                    collector.collect("对账成功：" + stringStringLongTuple3 + "  " + merchantValueState.value());
                    //清理状态
                    merchantValueState.clear();
                } else {
                    //更新状态
                    appValueState.update(stringStringLongTuple3);
                    //注册一个定时器，开始等待另一条流的时间   定时器为5秒
                    context.timerService().registerEventTimeTimer(stringStringLongTuple3.f2 + 5000L);
                }
            }

            @Override
            public void processElement2(Tuple4<String, String, String, Long> stringStringStringLongTuple4, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                //当前为merchant事件流,看另外一个流是否来过
                if (appValueState.value() != null) {
                    collector.collect("对账成功：" + stringStringStringLongTuple4 + "  " + appValueState.value());
                    //清理状态
                    appValueState.clear();
                } else {
                    //更新状态
                    merchantValueState.update(stringStringStringLongTuple4);
                    //注册一个定时器，开始等待另一条流的时间   定时器为5秒
                    context.timerService().registerEventTimeTimer(stringStringStringLongTuple4.f3);
                }
            }

            @Override
            public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                //定时器触发
                //如果某个状态不为空，说说明对账不成功，
                if (appValueState.value() != null) {
                    out.collect("对账失败：" + appValueState.value() + "第三方支付信息未到");

                }

                if (merchantValueState.value() != null) {
                    out.collect("对账失败：" + merchantValueState.value() + "app信息未到");

                }
                appValueState.clear();
                merchantValueState.clear();
            }
        }).print();

        executionEnvironment.execute();
    }
}
