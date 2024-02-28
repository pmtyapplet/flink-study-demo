package com.example.demo.tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/10/21 18:16
 */
public class TableConectionTest {

    public static void main(String[] args) {


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        String sql = "CREATE TABLE event (" +
                "platform_id STRING," +
                "url STRING" +
                ") WITH(" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/event.txt', " +
                " 'format' = 'csv' " +
                ")";

        tableEnvironment.executeSql(sql);
        Table event = tableEnvironment.from("event");

        Table resultTable = event.where($("platform_id").isEqual("applet")).select($("platform_id"), $("url"));

        tableEnvironment.createTemporaryView("resultTable", resultTable);


        resultTable.printSchema();

//        String sqlOut = "CREATE TABLE eventOut (" +
//                "user_name STRING," +
//                "url STRING" +
//                ") WITH(" +
//                " 'connector' = 'filesystem', " +
//                " 'path' = 'output', " +
//                " 'format' = 'csv' " +
//                ")";
//
//        tableEnvironment.executeSql(sqlOut);
//
//        resultTable.executeInsert("eventOut");

    }


}
