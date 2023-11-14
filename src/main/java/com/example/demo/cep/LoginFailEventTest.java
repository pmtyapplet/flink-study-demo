package com.example.demo.cep;

import com.example.demo.dot.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/11/8 19:08
 */
public class LoginFailEventTest {
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

        //定义模式，cep 调用 flink下的pattern
        Pattern<LoginEvent, LoginEvent> loginEventLoginEventPattern = Pattern.<LoginEvent>begin("first")
                //第一次登陆失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");

                    }
                })
                //第二次登录失败
                .next("second").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                    //第三次登录失败，  连续三次登录失败的事件
                }).next("third").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                });
        // 把模式应用到数据流上，监测复杂时间

        PatternStream<LoginEvent> patternStream = CEP.pattern( loginEventStream.keyBy(data->data.userId), loginEventLoginEventPattern);


        // 将监测到的复杂时间提取出来，进行处理得到报警信息输出
        SingleOutputStreamOperator<String> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                // 提取三次复杂时间中的失败时间
                LoginEvent first = map.get("first").get(0);
                LoginEvent second = map.get("second").get(0);
                LoginEvent third = map.get("third").get(0);
                return first.userId + "连续三次登录失败~ 登录时间：" + first.timestamp + "," + second.timestamp + "," + third.timestamp + ",";
            }
        });
        warningStream.print();
        env.execute();
    }
}
