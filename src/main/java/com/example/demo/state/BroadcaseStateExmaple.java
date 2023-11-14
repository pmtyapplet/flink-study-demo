package com.example.demo.state;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URI;


public class BroadcaseStateExmaple {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        //开启持久化
        executionEnvironment.enableCheckpointing(10000L);
        executionEnvironment.setStateBackend(new EmbeddedRocksDBStateBackend());


        executionEnvironment.setDefaultSavepointDirectory(new URI("hdfs:///rabbit1"));

        //检查点配置
        CheckpointConfig checkpointConfig = executionEnvironment.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(0);


        //用户的行为数据流
        DataStreamSource<Action> actionDataStreamSource = executionEnvironment.fromElements(
                new Action("李白", "读书"),
                new Action("李白", "写诗"),
                new Action("杜甫", "读书"),
                new Action("杜甫", "研磨")
        );
        //行为模式流，给予它构建广播流
        DataStreamSource<Pattern> patternDataStreamSource = executionEnvironment.fromElements(
                new Pattern("读书", "写诗"),
                new Pattern("读书", "研磨")
        );
        //定义广播状态描述起
        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<Void, Pattern>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> patternBroadcastStream = patternDataStreamSource.broadcast(descriptor);

        //链接两条流进行处理
        SingleOutputStreamOperator<Tuple2<String, Pattern>> matches = actionDataStreamSource.keyBy(data -> data.user).connect(patternBroadcastStream)
                .process(new MyBroadcastProcessFunction());
        matches.print();
        executionEnvironment.execute();
    }


    public static class MyBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> implements CheckpointedFunction {

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        }


        //定义一个keydState 保存上一次的用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-state", String.class));

        }

        @Override
        public void processElement(Action action, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Pattern>> collector) throws Exception {

            ReadOnlyBroadcastState<Void, Pattern> mapStateDescriptor = readOnlyContext.getBroadcastState(new MapStateDescriptor<>("MapStateDescriptor", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = mapStateDescriptor.get(null);
            //获取用户的上一次行为
            String preAction = prevActionState.value();
            //判断是否匹配
            if (pattern != null && preAction != null) {
                if (pattern.action1.equals(preAction) && pattern.action2.equals(action.action)) {
                    collector.collect(Tuple2.of(readOnlyContext.getCurrentKey(), pattern));

                }
            }
            prevActionState.update(action.action);
        }

        @Override
        public void processBroadcastElement(Pattern pattern, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context context, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            //从上下文中获取广播状态，饼用当前数据更新状态
            BroadcastState<Void, Pattern> broadcastState = context.getBroadcastState(new MapStateDescriptor<>("MapStateDescriptor", Types.VOID, Types.POJO(Pattern.class)));
            broadcastState.put(null, pattern);
        }
    }


    //定义用户行为和模式的 pojo

    public static class Action {
        public String user;
        public String action;

        public Action(String user, String action) {
            this.user = user;
            this.action = action;
        }

        public Action() {
        }

        @Override
        public String toString() {
            return "Action{" +
                    "user='" + user + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
