package com.example.demo.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2024/2/17 11:22
 */
public class FlinkCdcSqltest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment env = StreamTableEnvironment.create(executionEnvironment);


        env.executeSql("CREATE TABLE mysql_binlog(" +
                "    id       bigint ," +
                "    sku_id    bigint ," +
                "    warehouse_id  bigint," +
                "    PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "      'connector' = 'mysql-cdc'," +
                "      'hostname' = 'hadoop7'," +
                "      'port' = '3306'," +
                "      'username' = 'root'," +
                "      'password' = '123456'," +
                "      'database-name' = 'applet_user'," +
                "      'table-name' = 'ware_sku'" +
                "      );");

        Table table = env.sqlQuery("select * from mysql_binlog");
        table.printSchema();
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = env.toRetractStream(table, Row.class);
        //  DataStream<Row> dataStream = env.toDataStream(table);
        tuple2DataStream.print();
//        DataStream<org.apache.flink.types.Row> rowDataStream = env.toChangelogStream(table);
//        rowDataStream.print();

        executionEnvironment.execute();

    }
}
