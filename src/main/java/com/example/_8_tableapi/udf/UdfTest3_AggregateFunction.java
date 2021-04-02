package com.example._8_tableapi.udf;


import com.example.modul.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 聚合函数
 * @data: 2021/4/1 15:13 PM
 */
public class UdfTest3_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从文件读取数据
        DataStream<String> inputDataStream = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // 转换成 POJO
        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                }
        );

        // 将流转换成表
        Table sensorTable = tableEnv
                .fromDataStream(dataStream,
                        $("sensorId").as("id"),
                        $("timestamp").as("ts"),
                        $("temperature").as("temp"));
        // 注册 UDAGG 函数
        tableEnv
                .createTemporarySystemFunction("avgTemp", AvgTemp.class);

        // 自定义表函数 求当前传感器的平均温度值
        // table PAI
        Table resultTable = sensorTable
                .groupBy($("id"))
                .aggregate(call("avgTemp", $("temp")).as("avgTemp"))
                .select($("id"),
                        $("avgTemp"));

        sensorTable
                .groupBy("id")
                .aggregate("avgTemp(temp).as avgTemp")
                .select("id, avgTemp");

        resultTable.printSchema();
        tableEnv.toRetractStream(resultTable, Row.class).print("result");

        // SQL
        tableEnv.createTemporaryView("sensor",sensorTable);
        Table resultSqlTable = tableEnv
                .sqlQuery("select id,avgTemp(temp) as avgTemp " +
                        "from sensor " +
                        "group by id");
        resultSqlTable.printSchema();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的 AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        public void accumulate(Tuple2<Double, Integer> acc, Double temp) {
            acc.f0 += temp;
            acc.f1 += 1;
        }

    }
}
