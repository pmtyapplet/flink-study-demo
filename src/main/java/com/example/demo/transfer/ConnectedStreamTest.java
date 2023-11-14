package com.example.demo.transfer;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/22 19:46
 */
public class ConnectedStreamTest {
    public static void main(String[] args) throws Exception {
        //多流合并， 不同类型的流
        //此次讲不通类型的流 最终输出为同一个类型的流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> streamSource1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Long> streamSource2 = env.fromElements(6L, 7L, 8L, 9L, 10L);
        SingleOutputStreamOperator<String> map = streamSource1.connect(streamSource2).map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer integer) throws Exception {

                return "integer:" + integer.toString();
            }


            @Override
            public String map2(Long aLong) throws Exception {
                return "long:" + aLong.toString();
            }
        });
        map.print();
        env.execute();
    }
}
