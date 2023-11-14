package com.example.demo.tableapiUDF;

import com.example.demo.dot.Event;
import com.example.demo.dot.SourceFun;
import lombok.Data;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/10/31 14:29
 */
public class UserDefFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<Event> stream = env.addSource(new SourceFun())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );

        Table table = tableEnv.fromDataStream(stream, $("username"), $("url"), $("timestamp").as("et"), $("timestamp").rowtime().as("ts"));
        tableEnv.registerTable("event", table);
        //注册到环境中
        tableEnv.createTemporaryFunction("MyHash", MyHashFunction.class);
        tableEnv.createTemporaryFunction("MyTable", MyTableFunction.class);
        tableEnv.createTemporaryFunction("MyWeighted", MyWeightedAggregateFunction.class);

        //使用 函数
        Table queryScalar = tableEnv.sqlQuery("select username,MyHash(username) from event");

        Table queryTable = tableEnv.sqlQuery("select username,url,word,length from event,LATERAL TABLE( MyTable(url)) as T( word,length)");

        Table queryAgg = tableEnv.sqlQuery("select username,MyWeighted(et,1) as weight from event group by username ");


        // tableEnv.toChangelogStream(queryScalar).print();
        //tableEnv.toChangelogStream(queryTable).print();
        tableEnv.toChangelogStream(queryAgg).print();

        env.execute();
    }

    // 注册自定义标量函数

    //调用UDF进行查询转换

    //转换成流打印数据


    // 定义标量函数
    public static class MyHashFunction extends ScalarFunction {
        // 没有重写的方法，名字固定为 eval
        public int eval(String str) {

            return str.hashCode();
        }

    }


    //定义表函数
    public static class MyTableFunction extends TableFunction<Tuple2<String, Integer>> {
        // 没有重写的方法，名字固定为 eval
        public void eval(String str) {
            String[] split = str.split("\\.");
            for (String s : split) {
                //用上面集成来的collect 方法收集成一个list  返回的即为一个表
                collect(Tuple2.of(s, s.length()));
            }
        }

    }

    @Data
    public static class WeightedAggregatePojo {
        public long sum = 0;
        public long count = 0;

    }

    //聚合函数
    //计算加权平均值
    public static class MyWeightedAggregateFunction extends AggregateFunction<Long, WeightedAggregatePojo> {

        @Override
        public Long getValue(WeightedAggregatePojo weightedAggregatePojo) {
            if (weightedAggregatePojo.sum == 0)
                return null;
            else
                return weightedAggregatePojo.sum / weightedAggregatePojo.count;

        }

        @Override
        public WeightedAggregatePojo createAccumulator() {
            return new WeightedAggregatePojo();
        }

        //累加计算的方法,没有重写的方法，必须是 这个名字的方法
        public void accumulate(WeightedAggregatePojo weightedAggregatePojo, Long value, Long weight) {
            weightedAggregatePojo.sum += value * weight;
            weightedAggregatePojo.count += weight;
        }
    }

}
