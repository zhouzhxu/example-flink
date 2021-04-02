package com.example._8_tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:28 PM
 */
public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {

        // 1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接 kafka 读取数据
        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("sensorId", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable"); // 注册表

        // 3.查询转换
        Table sensorTable = tableEnv.from("inputTable");

        // 简单转换
        Table resultTable = sensorTable
                // 使用 Expressions 表达式
                .select($("sensorId"), $("timestamp"), $("temperature"));

        tableEnv.toAppendStream(resultTable, Row.class).print();

        // 聚合统计
        Table aggTable = sensorTable
                .groupBy($("sensorId"))
                .select($("sensorId"),
                        $("sensorId")
                                .count()
                                .as("count"),
                        $("temperature")
                                .avg()
                                .as("avgTemp"));

        // 建立 kafka 连接，输出到指定的主题
        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("sinktest")
                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("sensorId", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable"); // 注册表

        resultTable.insertInto("outputTable");

        tableEnv.execute("outputKafka");
//        env.execute();

    }
}
