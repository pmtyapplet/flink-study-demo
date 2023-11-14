package com.example.demo.TopN;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/20 18:14
 */
public class TopNTest {
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

        //按照url分组，统计窗口内每个url访问量

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Event event, Long aLong) {
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
                }, new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        Long next = iterable.iterator().next();
                        collector.collect(new UrlViewCount(start, end, next, s));
                    }
                });
        urlCountStream.print("url count ");

        //对于统一窗口统计出的访问量进行收集和排序
        SingleOutputStreamOperator<String> process = urlCountStream.keyBy(data -> data.end)
                .process(new KeyedProcessFunction<Long, UrlViewCount, String>() {
                    //定义一个属性N
                    private Integer n;
                    //定义一个列表状态
                    private ListState<UrlViewCount> urlViewCountListState;

                    //在环境中获取状态
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-count-list", UrlViewCount.class));
                    }

                    @Override
                    public void processElement(UrlViewCount urlViewCount, KeyedProcessFunction<Long, UrlViewCount, String>.Context context, Collector<String> collector) throws Exception {
                        //将数据保存到状态中
                        urlViewCountListState.add(urlViewCount);
                        //注册windowEnd+1ms的定时器
                        context.timerService().registerEventTimeTimer(context.getCurrentKey() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<UrlViewCount> urlViewCounts = new ArrayList<>();
                        //真正做计算的过程

                        for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                            urlViewCounts.add(urlViewCount);
                        }

                        urlViewCounts.sort(((o1, o2) -> o2.count.intValue() - o1.count.intValue()));

                        //包装信息

                        StringBuffer result = new StringBuffer();
                        result.append("--------------------------");
                        result.append("\n" + "s窗口结束时间： " + new Timestamp(ctx.getCurrentKey()) + "\n");
                        //取list前两个，包装信息输出
                        for (int i = 0; i < (Math.min(urlViewCounts.size(), 2)); i++) {
                            UrlViewCount urlViewCount = urlViewCounts.get(i);
                            String info = "No. " + (i + 1) + " "
                                    + "url: " + urlViewCount.getUrl() + " "
                                    + "访问量： " + urlViewCount.getCount() + "\n";
                            result.append(info);
                        }
                        result.append("--------------------------");
                        out.collect(result.toString());
                    }
                });
        process.print();

        env.execute();
    }


}
