package com.example._8_tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:27 PM
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
//
        // 3.查询转换
        // 3.1 Table API
        // 3.1.1 简单转换
        Table resultTable = inputTable
                // 使用 Expressions 表达式
                .select($("sensorId"), $("timestamp"), $("temperature"))
                // 过滤出符合要求的数据 获取sensorId为sensor_9的数据
                .filter($("sensorId").isEqual("sensor_9"));

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

        // 3.2 SQL
        Table sqlTable = tableEnv.sqlQuery("select * from inputTable where sensorId = 'sensor_10'");

        Table sqlAggTable = tableEnv.sqlQuery("select sensorId, " +
                "count(sensorId) as cnt, avg(temperature) as avgTemp from inputTable group by sensorId ");

        // 4.输出到文件
        // 连接外部文件系统注册输出表
        String outputPath = "D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\outputSensor.txt";

        tableEnv
                .connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("sensorId", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable"); // 注册表

        resultTable.insertInto("outputTable");


        tableEnv.execute("");
//        env.execute();
    }
}
