package com.example._8_tableapi;

import com.example.modul.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:26 PM
 */
public class TableTest1_Example {
    public static void main(String[] args) throws Exception {

        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度为1

        // 2.从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // 3.转换成POJO类型
        DataStream<SensorReading> dataStream = inputStream
                // 过滤空行
                .filter(line -> !StringUtils.isNullOrWhitespaceOnly(line))
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
                });


        // 4.创建流表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 5.基于流创建表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 6.调用table API 进行转换操作
        Table resultTable = dataTable
                // 使用 Expressions
                .select($("sensorId"),$("timestamp"),$("temperature"))
                .where($("sensorId").isEqual("sensor_1"));

        // 7.执行SQL 先注册表
        tableEnv.createTemporaryView("sensor", dataStream);
        String sql = "select * from sensor where sensorId = 'sensor_1' ";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        // 8.打印输出结果
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();

    }
}
