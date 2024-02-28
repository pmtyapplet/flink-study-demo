package com.example.demo.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pm.resp.LoginResp;
import com.pm.service.GameService;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2024/2/18 15:26
 */
public class KafkaToDbTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> stringDataStreamSource = env.fromSource(KafkaSource.<String>builder()
                .setBootstrapServers("hadoop1:9092")
                //取的是 kafka配置中的 /opt/module/kafka/config/consumer.properties
                .setGroupId("test-consumer-group")
                .setTopics("applet_flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build(), WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "kafka_source");


        stringDataStreamSource.print("kafka source input：");


        OutputTag<String> dirtyData = new OutputTag<String>("dirtyData") {
        };

        SingleOutputStreamOperator<JSONObject> jsonDs = stringDataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //脏数据 发送到侧输出流
                    context.output(dirtyData, value);
                }

            }
        });


        SingleOutputStreamOperator<JSONObject> map = jsonDs.keyBy(jsonObject -> {

            String op = jsonObject.getString("op");
            if (op.equals("c")) {
                return jsonObject.getJSONObject("after").getLong("id");


            } else if (op.equals("d")) {
                return jsonObject.getJSONObject("before").getLong("id");
            } else {
                return jsonObject.getJSONObject("after").getLong("id");
            }
        }).map(new RichMapFunction<JSONObject, JSONObject>() {


            private ValueState<String> valueState;


            @Override
            public void open(Configuration parameters) throws Exception {

                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNewState", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                JSONObject after = jsonObject.getJSONObject("after");
                Long id;
                Long sku_id;
                if (after != null) {
                    id = after.getLong("id");
                    sku_id = after.getLong("sku_id");
                } else {
                    JSONObject before = jsonObject.getJSONObject("before");
                    id = before.getLong("id");
                    sku_id = before.getLong("sku_id");
                }

                jsonObject.put("id", id);

                if (!id.equals(sku_id)) {
                    String value = valueState.value();
                    if (value == null) {
                        jsonObject.put("sku_id", id);
                        valueState.update(id.toString());
                    }
                }
                return jsonObject;
            }
        });


        OutputTag<String> evenOutputTag = new OutputTag<String>("evenData") {
        };


        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = map.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {

                //如果是偶数  将数据写入测输出流
                if ((jsonObject.getLong("id") & 2) == 0) {
                    context.output(evenOutputTag, jsonObject.toJSONString());
                } else {
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });

        SideOutputDataStream<String> sideOutput = stringSingleOutputStreamOperator.getSideOutput(evenOutputTag);

        stringSingleOutputStreamOperator.print("主流>>>");
        sideOutput.print("测输出流>>>");
        jsonDs.getSideOutput(dirtyData).print("我是脏数据》》》");
        stringSingleOutputStreamOperator.sinkTo(getKafkaSink("我是个主流 topic"));
        sideOutput.sinkTo(getKafkaSink("我是个测输出流 topic"));


        env.execute();
    }

    public static KafkaSink<String> getKafkaSink(String topicName) {


        return KafkaSink.<String>builder()
                .setBootstrapServers("hadoop1:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("applet-" + topicName + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

}

