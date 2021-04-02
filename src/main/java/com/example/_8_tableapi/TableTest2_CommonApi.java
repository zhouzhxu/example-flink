package com.example._8_tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:27 PM
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings =
                EnvironmentSettings
                        .newInstance()
                        .useOldPlanner()
                        .inStreamingMode()
                        .build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        // 1.2 基于老版本planner的批处理
        ExecutionEnvironment bacthEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBacthTableEnv = BatchTableEnvironment.create(bacthEnv);

        // 1.3 基于Blink的流处理  环境设定 使用blink计划器，在流媒体模式下构建
        EnvironmentSettings blinkStreamSettings =
                EnvironmentSettings
                        .newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 1.4 基于Blink的批处理
        EnvironmentSettings blinkBacthSettings =
                EnvironmentSettings
                        .newInstance()
                        .useBlinkPlanner()
                        .inBatchMode()
                        .build();
        TableEnvironment blinkBacthTableEnv = TableEnvironment.create(blinkBacthSettings);

        String filePath = "D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt";
        // 2.表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        tableEnv
                .connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("sensorId", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable"); // 注册表

        Table inputTable = tableEnv
                .from("inputTable");

//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3.查询转换
        // 3.1 Table API
        // 3.1.1 简单转换
        Table resultTable = inputTable
                // 使用 Expressions 表达式
                .select($("sensorId"), $("timestamp"), $("temperature"))
                // 过滤出符合要求的数据 获取sensorId为sensor_9的数据
                .filter($("sensorId").isEqual("sensor_9"));
//        tableEnv.toAppendStream(resultTable, Row.class).print();

        // 3.1.2 聚合统计
        Table aggTable = inputTable
                .groupBy($("sensorId"))
                .select($("sensorId"),
                        $("sensorId")
                                .count()
                                .as("count"),
                        $("temperature")
                                .avg()
                                .as("avgTemp"));
//        avgTable.printSchema();
//        tableEnv.toAppendStream(avgTable, Row.class).print();
        tableEnv.toRetractStream(aggTable, Row.class).print();

        // 3.2 SQL
        Table sqlTable = tableEnv.sqlQuery("select * from inputTable where sensorId = 'sensor_10'");
//        tableEnv.toAppendStream(sqlTable, Row.class).print();

        Table sqlAggTable = tableEnv.sqlQuery("select sensorId, " +
                "count(sensorId) as cnt, avg(temperature) as avgTemp from inputTable group by sensorId ");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print();

        env.execute();
    }
}
