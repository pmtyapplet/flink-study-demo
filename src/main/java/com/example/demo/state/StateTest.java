package com.example.demo.state;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
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

        streamOperator.keyBy(data -> data.username).flatMap(
                new RichFlatMapFunction<Event, String>() {
                    //定义状态
                    ValueState<Event> valueState;
                    ListState<Event> listState;
                    MapState<String, Long> mapState;
                    ReducingState<Event> reducingState;
                    AggregatingState<Event, String> aggregatingState;


                    @Override
                    public void open(Configuration parameters) throws Exception {

                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("event-value-state", Event.class));
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("event-list-state", Event.class));
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("event-map-state", String.class, Long.class));
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("event-reducing-state",
                                new ReduceFunction<Event>() {
                                    @Override
                                    public Event reduce(Event event, Event t1) throws Exception {
                                        //event为已经聚合到的数据，t1 为当前进入到聚合方法中的数据
                                        //数据返回为拿到新的聚合的到的值是什么，
                                        //当前的处理仅为 更新最新的时间戳 将数据返回出去
                                        return new Event(event.username, event.url, t1.timestamp);
                                    }
                                }
                                , Event.class));
                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("event-agg-state",
                                new AggregateFunction<Event, Long, String>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(Event event, Long aLong) {
                                        return aLong + 1;
                                    }

                                    @Override
                                    public String getResult(Long aLong) {
                                        return "当前计数=count:" + aLong;
                                    }

                                    @Override
                                    public Long merge(Long a, Long b) {
                                        return a + b;
                                    }
                                }
                                , Long.class));
                    }

                    @Override
                    public void flatMap(Event event, Collector<String> collector) throws Exception {
                        //访问和更新状态
                        System.out.println(valueState.value());
                        valueState.update(event);
                        System.out.println("valueState = " + valueState.value());

                        listState.add(event);

                        //初始化值应该是1  因为当前已经进入了一条数据了
                        mapState.put(event.username, mapState.get(event.username) == null ? 1 : mapState.get(event.username) + 1);
                        System.out.println("mapState = " + event.username + mapState.get(event.username));

                        aggregatingState.add(event);
                        System.out.println("aggregatingState = " + aggregatingState.get());

                        reducingState.add(event);
                        System.out.println("reducingState = " + reducingState.get());
                    }
                }
        ).print();
        env.execute();
    }
}
