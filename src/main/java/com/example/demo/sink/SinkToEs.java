package com.example.demo.sink;

import com.example.demo.dot.Event;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBuilderBase;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/7/18 10:32
 */
public class SinkToEs {

//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        DataStreamSource<Event> stream = env.fromElements(
//                new Event("Mary", "", 1000L),
//                new Event("Bob", "", 2000L),
//                new Event("ALice", "", 3000L),
//                new Event("Bob", "", 3300L),
//                new Event("ALice", "", 3200L),
//                new Event("Bob", "", 3400L),
//                new Event("Bob", "", 3800L),
//                new Event("Bob", "", 4200L)
//        );
//
//        List<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("192.168.10.103",9200));
//
//        ElasticsearchSinkBuilderBase<Object, B> objectBElasticsearchSinkBuilderBase = new ElasticsearchSinkBuilderBase<>();
//        ElasticsearchSink.Builder<Event> eventBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<Event>() {
//            @Override
//            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
//
//                HashMap<String, String> map = new HashMap<>();
//                map.put(event.username,event.url);
//                Requests.indexRequest().index("event").type("type").source(map);
//            }
//        });
//
//
//        ElasticsearchSink<Event> build = eventBuilder.build();
//        stream.addSink(build);
//        env.execute();
//    }
}
