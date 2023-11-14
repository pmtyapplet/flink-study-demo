package com.example.demo.sink;

import com.example.demo.dot.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/7/22 22:32
 */
public class SinkToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder jdbcConnectionOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        jdbcConnectionOptionsBuilder.withUrl("jdbc:mysql://172.18.178.134:6606/panda_auth?useUnicode=true&useSSL=false&");
        jdbcConnectionOptionsBuilder.withDriverName("com.mysql.cj.jdbc.Driver");
        jdbcConnectionOptionsBuilder.withUsername("root");
        jdbcConnectionOptionsBuilder.withPassword("12345678");

        DataStreamSink<Event> eventDataStreamSink = stream.addSink(JdbcSink.sink(
                "insert into event(username,url) values(?,?)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        //此处的1  2  分别对前面的占位符
                        preparedStatement.setString(1, event.username);
                        preparedStatement.setString(2, event.url);
                    }
                },
                jdbcConnectionOptionsBuilder.build()
        ));
        env.execute()
        ;
    }
}
