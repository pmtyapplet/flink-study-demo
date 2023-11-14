package com.example.demo.cep;

import com.example.demo.dot.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/11/12 21:18
 */
public class LoginFailEventTest2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                new LoginEvent("李白", "192.168.0.1", "fail", 2000L),
                new LoginEvent("李白", "192.168.0.2", "fail", 3000L),
                new LoginEvent("杜甫", "192.168.1.29", "fail", 4000L),
                new LoginEvent("李白", "172.56.23.10", "fail", 5000L),
                new LoginEvent("杜甫", "192.168.1.29", "success", 6000L),
                new LoginEvent("杜甫", "192.168.1.29", "fail", 7000L),
                new LoginEvent("杜甫", "192.168.1.29", "fail", 8000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
            @Override
            public long extractTimestamp(LoginEvent loginEvent, long l) {
                return loginEvent.timestamp;
            }
        }));

        Pattern<LoginEvent, LoginEvent> loginEventLoginEventPattern = Pattern.<LoginEvent>begin("fail")
                //第一次登陆失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");

                    }
                }).times(3).consecutive();
        PatternStream<LoginEvent> pattern = CEP.pattern(loginEventStream.keyBy(data->data.userId), loginEventLoginEventPattern);

        SingleOutputStreamOperator<String> process = pattern.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<String> collector) throws Exception {
                LoginEvent first = map.get("fail").get(0);
                LoginEvent second = map.get("fail").get(1);
                LoginEvent third = map.get("fail").get(2);
                collector.collect(first.userId + "连续三次登录失败~ 登录时间：" + first.timestamp + "," + second.timestamp + "," + third.timestamp + ",");
            }
        });
        process.print();
        env.execute();
    }
}
