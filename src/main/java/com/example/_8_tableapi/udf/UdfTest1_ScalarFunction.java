package com.example._8_tableapi.udf;

import com.example.modul.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 标量函数
 * @data: 2021/4/1 14:24 PM
 */
public class UdfTest1_ScalarFunction {

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

        // 自定义标量函数 实现求sensorId的hash值
        // table API
        HashCode hashCode = new HashCode(1);
        // 在环境中注册UDF
//        tableEnv.registerFunction("hashCode",hashCode); // 不推荐使用
        tableEnv.createTemporarySystemFunction("hashCode", hashCode);
        Table resultTable = sensorTable
                .select($("id"),
                        $("ts"),
                        $("temp"),
                        call("hashCode", $("id")).as("hcId"));
        sensorTable
                .select("id,ts,temp,hashCode(id) as hcId");

        resultTable.printSchema();
        tableEnv.toAppendStream(resultTable, Row.class).print("result");

        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id,ts,temp,hashCode(id) as hcId from sensor");
        resultSqlTable.printSchema();
        tableEnv.toAppendStream(resultTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的scalarFunction
    public static class HashCode extends ScalarFunction {

        private Integer factor = 3;

        public HashCode(Integer factor) {
            this.factor = factor;
        }

        // 接收任意类型的数据并返回一个Int
        public Integer eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode() * factor;
        }

    }

}
