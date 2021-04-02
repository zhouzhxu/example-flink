package com.example._8_tableapi;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 利用connect无法向es写入数据，未找到原因
 * @data: 2021/3/31 12:28 PM
 */
public class TableTest10_ES {
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

        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");

        // 3.1.2 聚合统计
        Table aggTable = sensorTable
                .groupBy($("sensorId"))
                .select($("sensorId"),
                        $("sensorId")
                                .count()
                                .as("count"),
                        $("temperature")
                                .avg()
                                .as("avgTemp"));
        tableEnv.toRetractStream(aggTable, Row.class).print("aggTable");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(aggTable, Row.class);

        // 4. 建立 ES 连接
        // 4.1 追加写入到ES中
        /*tableEnv
                .connect(new Elasticsearch()
                        .version("7")
                        .host("localhost", 9200, "http")
                        .index("sensor")
                        .documentType("sensor"))
                .withFormat(new Json())
                .withSchema(new Schema().field("sensorId", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");*/

        // 4.2 在Upsert模式下写入ES
        tableEnv
                .connect(new Elasticsearch()
                        .version("7")
                        .host("localhost", 9200, "http")
                        .index("sensor")
                        .documentType("_doc")
                        .keyNullLiteral("null")
                        .keyDelimiter("_"))
                .withFormat(new Json()
                        /*.jsonSchema("{" +
                                "'type': 'object'," +
                                "'properties':{" +
                                "'sensorId':{" +
                                "'type':'string'" +
                                "}" +
                                "'timestamp':{" +
                                "'type':'number'" +
                                "}" +
                                "'temperature':{" +
                                "'type':'number'" +
                                "}" +
                                "}")*/)
                .withSchema(new Schema()
                        .field("sensorId", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT().toString())
                        .field("temperature", DataTypes.DOUBLE().toString()))
                .inUpsertMode()
                .createTemporaryTable("outputTable");

//        resultTable.insertInto("outputTable");
        aggTable.insertInto("outputTable");

//        tableEnv.insertInto(resultTable,"outputTable");

//        tableEnv.execute("outputES");

        env.execute();
    }
}
