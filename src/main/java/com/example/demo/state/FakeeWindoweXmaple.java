package com.example.demo.state;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;

public class FakeeWindoweXmaple {
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
        streamOperator.keyBy(data -> data.url)
                .process(new FakeWindowProcessResult(10000L)).print();

        env.execute();

    }

    public static class FakeWindowProcessResult extends KeyedProcessFunction<String, Event, String> {

        private Long windowSize;

        public FakeWindowProcessResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        //定义一个mapState 用来保存窗口中统计的count 值
        MapState<Long, Long> windowUrlCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            //来一条数据 根据时间戳判断 属于那个窗口，窗口分配
            long start = event.timestamp / windowSize * windowSize;
            long end = start + windowSize;

            //注册end-1的定时器
            context.timerService().registerEventTimeTimer(end - 1);
            //更新状态，进行增量聚合
            if (windowUrlCountMapState.contains(start)) {
                Long count = windowUrlCountMapState.get(start);
                windowUrlCountMapState.put(start, count + 1);

            } else {
                windowUrlCountMapState.put(start, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;
            Long count = windowUrlCountMapState.get(windowStart);
            out.collect("窗口" + new Timestamp(windowStart) + " ~ " + new Time(windowEnd) + "  url:" + ctx.getCurrentKey() + "count: " + count);

            windowUrlCountMapState.remove(windowStart);
        }
    }
}
