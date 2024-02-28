package com.example.demo.TopN;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/20 18:53
 */
public class TopNProcessAllWindow {
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
       //直接开窗，收集所有数据
        stream.map(data -> data.username).windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
                .print();


        //对于同意窗口统计出的访问量进行收集和排序

        env.execute();
    }

    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String s, HashMap<String, Long> stringLongHashMap) {
            if (stringLongHashMap.containsKey(s)) {
                Long aLong = stringLongHashMap.get(s);
                stringLongHashMap.put(s, aLong + 1);
            } else {
                stringLongHashMap.put(s, 1L);
            }
            return stringLongHashMap;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> stringLongHashMap) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            for (String key : stringLongHashMap.keySet()) {
                result.add(Tuple2.of(key, stringLongHashMap.get(key)));
            }
            result.sort(((o1, o2) -> o2.f1.intValue() - o1.f1.intValue()));
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }
    }


    //实现自定义全窗口函数，包装喜喜输出结果
    public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Long>>> iterable, Collector<String> collector) throws Exception {
            ArrayList<Tuple2<String, Long>> list = iterable.iterator().next();
            StringBuffer result = new StringBuffer();
            result.append("--------------------------");
            result.append("\n" + "s窗口结束时间： " + new Timestamp(context.window().getEnd()) + "\n");
            //取list前两个，包装信息输出
            for (int i = 0; i < (Math.min(list.size(), 2)); i++) {
                Tuple2<String, Long> currentTuple = list.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + currentTuple.f0 + " "
                        + "访问量： " + currentTuple.f1 + "\n";
                result.append(info);
            }
            result.append("--------------------------");
            collector.collect(result.toString());
        }
    }
}
