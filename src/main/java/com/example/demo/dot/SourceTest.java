package com.example.demo.dot;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.tools.nsc.doc.model.Class;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author applet
 * @date 2023/5/21
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);


        DataStreamSource<Event> eventDataStreamSource1 = executionEnvironment.addSource(new SourceFun());
        //shuffle 随即分区
        eventDataStreamSource1.shuffle().print().setParallelism(4);
        //轮询
        eventDataStreamSource1.rebalance().print().setParallelism(4);

        eventDataStreamSource1.print();
        executionEnvironment.execute();
        Map<String, String> stringStringHashMap = new HashMap<>();
    }
}
