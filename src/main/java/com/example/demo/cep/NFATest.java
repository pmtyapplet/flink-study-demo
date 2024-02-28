package com.example.demo.cep;

import com.example.demo.dot.LoginEvent;
import com.example.demo.dot.OrderEvent;
import lombok.Data;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/11/13 19:48
 */
public class NFATest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<LoginEvent, String> keyedStream = env.fromElements(
                new LoginEvent("李白", "order-1", "create", 1000L),
                new LoginEvent("杜甫", "order-2", "create", 2 * 1000L),
                new LoginEvent("李白", "order-1", "modify", 10 * 1000L),
                new LoginEvent("李白", "order-1", "pay", 60 * 1000L),
                new LoginEvent("杜甫", "order-3", "create", 10 * 60 * 1000L),
                new LoginEvent("杜甫", "order-3", "pay", 20 * 60 * 1000L)
        ).keyBy(LoginEvent::getUserId);

        //按照顺序依次输入，用状态机进行处理，状态跳转
        SingleOutputStreamOperator<String> warningStream = keyedStream.flatMap(new StateMachineMapper());

        warningStream.print();
        env.execute();
    }

    public static class  StateMachineMapper extends RichFlatMapFunction<LoginEvent,String>{

        //生命状态机的当前状态
        ValueState<State> currencyState;

        @Override
        public void open(Configuration parameters) throws Exception {

            currencyState=getRuntimeContext().getState(new ValueStateDescriptor<State>("state",State.class));
        }

        @Override
        public void flatMap(LoginEvent loginEvent, Collector<String> collector) throws Exception {

            //如果状态为null 进行初始化
            State state=currencyState.value();
            if (state==null){
                state=State.init;
            }
            //跳转到下一个状态
            State nextTransition = state.transition(loginEvent.eventType);
            //判断当前状态的特殊情况，直接进行跳转
            if (nextTransition==State.Match){
                //监测到了匹配，输出报警信息，不更新状体哎 就是跳转回S2

                collector.collect(loginEvent.userId+"连续三次登录失败");
            }else  if (nextTransition ==State.Terminal){
                //直接将状态更新为初始状态，重新开始监测
                State init = State.init;
                currencyState.update(State.init);
            }else {
                currencyState.update(nextTransition);
            }
        }
    }

    public static enum State{
        Terminal,//匹配失败，终止状态
        Match,
        //匹配成功

        S2(new Transition("fail",Match),new Transition("fail",Terminal)),
        S1(new Transition("fail",S2),new Transition("fail",Terminal)),

        init(new Transition("fail",S1),new Transition("fail",Terminal));

        private Transition[] transitions;

        State(Transition... transitions) {
            this.transitions=transitions;
        }

        public State transition(String eventType) {
            for (Transition transition : transitions) {
                if (transition.getEventType().equals(eventType)){
                    return transition.getTargetState();

                }
            }
            //直接回到初始化
            return init;
        }
    }
    @Data
    public static class Transition{
        private String eventType;
        private State targetState;

        public Transition() {

        }

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }
    }
}
