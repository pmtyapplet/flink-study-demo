package com.example.demo.cep;

import com.example.demo.dot.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/11/12 21:48
 */
public class OrderTimeOutDetectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = env.fromElements(
                new OrderEvent("李白", "order-1", "create", 1000L),
                new OrderEvent("杜甫", "order-2", "create", 2 * 1000L),
                new OrderEvent("李白", "order-1", "modify", 10 * 1000L),
                new OrderEvent("李白", "order-1", "pay", 60 * 1000L),
                new OrderEvent("杜甫", "order-3", "create", 10 * 60 * 1000L),
                new OrderEvent("杜甫", "order-3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent orderEvent, long l) {
                return orderEvent.timestamp;
            }
        }));

        //定义模式CEP

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")

                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("pay");
                    }
                }).within(Time.minutes(15));
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventSingleOutputStreamOperator.keyBy(data -> data.orderId), pattern);

        //定义一个测输出流标签
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        //如果要处理迟到数据 可以通过 sideOutputLateData 定义一个测输出标签 再次进行模式匹配
        //OutputTag<OrderEvent> lateDataTag = new OutputTag<OrderEvent>("late-data"){};
        //patternStream.sideOutputLateData(lateDataTag).select()

        //查询匹配到的数据
        SingleOutputStreamOperator<String> result = patternStream.process(new OrderPayMatch());

        result.print("payed:");
        result.getSideOutput(timeoutTag).print("timeout:");
env.execute();

    }

    public static class OrderPayMatch extends PatternProcessFunction<OrderEvent,String> implements TimedOutPartialMatchHandler<OrderEvent>{
        //正常匹配
        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {
            //获取当前的支付时间
            OrderEvent orderEvent = map.get("pay").get(0);
            collector.collect("用户："+orderEvent.userId+"订单"+orderEvent.orderId+"已支付");
        }

        //超时匹配 重点是  实现TimedOutPartialMatchHandler
        @Override
            public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {
            OrderEvent create = map.get("create").get(0);
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
            context.output(timeoutTag,"用户："+create.userId+"订单"+create.orderId+"已超时支付");
        }
    }

    //处理迟到数据 使用测输出流

}
