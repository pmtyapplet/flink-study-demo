package com.example.demo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

/**
 * @author applet
 * @date 2023/5/15
 */
public class StreamWordCountListern {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localhost = executionEnvironment.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = localhost.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String word : split) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {

                return Tuple2.of(s, 1L);
            }
        }).keyBy(data -> data.f0).sum(1);
        //随即分区
        sum.shuffle();
        //轮询分区
        sum.rebalance();
        //重缩放
        sum.rescale();

        //广播分区，每一个算子都会处理一次
        sum.broadcast();

        //全局分区 把所有的数据全部分到一个算子中
        sum.global();

        sum.print();

        // 启动执行

        executionEnvironment.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {

                for (int i = 1; i <= 8; i++) {

                    //将奇偶数分别发送到0、1号进行分区
                    //将值为运行时上下文的任务值
                    if (i % 2 == getIterationRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i);
                    }
                }
            }


            @Override
            public void cancel() {

            }
        }).setParallelism(2);

        //自定义分区
        sum.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int i) {
                return 0;
            }
        }, new KeySelector<Tuple2<String, Long>, Integer>() {
            @Override
            public Integer getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return null;
            }
        }).rebalance();

        executionEnvironment.execute();
    }
}
