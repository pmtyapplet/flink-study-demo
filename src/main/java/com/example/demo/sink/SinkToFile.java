package com.example.demo.sink;


import com.example.demo.dot.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/7/17 22:07
 */
public class SinkToFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //写入到文件只能用单独的流来处理  所以并行度只能是1

        env.setParallelism(4);


        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "", 1000L),
                new Event("Bob", "", 2000L),
                new Event("ALice", "", 3000L),
                new Event("Bob", "", 3300L),
                new Event("ALice", "", 3200L),
                new Event("Bob", "", 3400L),
                new Event("Bob", "", 3800L),
                new Event("Bob", "", 4200L)
        );


        StreamingFileSink<String> build = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(15)).build()
                ).build();


        stream.map(data -> data.toString()).addSink(build);
        env.execute();
    }
}
