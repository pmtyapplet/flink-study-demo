package com.example.demo.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/10/21 12:34
 */
public class CommonTableApiTest {
    public static void main(String[] args) {
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        executionEnvironment.setParallelism(1);
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        //定义环境配置来创建表执行环境

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        TableEnvironment environment = TableEnvironment.create(settings);
    }
    /**
     * B端报表
     * 1.数据采集加白不通，测试流程不通
     * 2.异常回滚
     * 2.1：离线服务异常，离线服务异常信息不影响历史版本离线数据，
     * 2.2：报表查询异常，报表查询异常信息，回滚旧版本可正常使用
     * 2.3：数据库脚本回滚，数据库脚本无序回滚
     * 2.4：体育报表接口无法采集数据，发版期间调通
     *
     * B端报表服务风险报告
     * 1.目前数据还无法采集，测试流程还未通
     */


}
