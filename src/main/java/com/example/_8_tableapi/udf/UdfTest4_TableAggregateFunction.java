package com.example._8_tableapi.udf;

import com.example.modul.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 表聚合函数
 * @data: 2021/4/1 17:21 PM
 */
public class UdfTest4_TableAggregateFunction {
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
        // 注册 UDTAGG 函数
        tableEnv
                .createTemporarySystemFunction("Top3", Top3.class);

        // 自定义表函数 求当前传感器的平均温度值
        // table PAI
        Table resultTable = sensorTable
                .groupBy($("id"))
                .flatAggregate(call("Top3", $("temp")))
                .select($("id"),
                        $("f0").as("top"),
                        $("f1").as("temp"));

        /*sensorTable
                .groupBy("id")
                .aggregate("Top3(temp).as top")
                .select("id, top");*/

        resultTable.printSchema();
        tableEnv.toRetractStream(resultTable, Row.class).print("result");

        // SQL
        /*tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv
                .sqlQuery("select id,avgTemp(temp) as avgTemp " +
                        "from sensor " +
                        "group by id");
        resultSqlTable.printSchema();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");*/

        env.execute();
    }

    // 实现自定义的 TableAggregateFunction 取前三
    public static class Top3 extends TableAggregateFunction<Tuple2<Integer, Double>,
            Tuple3<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>> {

        @Override
        public Tuple3<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> createAccumulator() {
            return new Tuple3<>(new Tuple2<>(Integer.MIN_VALUE, Double.MIN_VALUE),
                    new Tuple2<>(Integer.MIN_VALUE, Double.MIN_VALUE),
                    new Tuple2<>(Integer.MIN_VALUE, Double.MIN_VALUE));
        }

        public void accumulate(Tuple3<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> acc,
                               Double value) {
            if (value > acc.f0.f1) {
                acc.f0.f1 = value;
                acc.f0.f0 = 1;
            } else if (value > acc.f1.f1) {
                acc.f1.f1 = value;
                acc.f1.f0 = 2;
            } else if (value > acc.f2.f1) {
                acc.f2.f1 = value;
                acc.f2.f0 = 3;
            }
        }

        public void merge(Tuple3<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> acc,
                          Iterable<Tuple3<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>> it) {
            for (Tuple3<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> a : it) {
                accumulate(acc, a.f0.f1);
                accumulate(acc, a.f1.f1);
                accumulate(acc, a.f2.f1);
            }
        }

        public void emitValue(Tuple3<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> acc,
                              Collector<Tuple2<Integer, Double>> out) {
            // emit the value and rank
            if (acc.f0.f0 != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f0.f0, acc.f0.f1));
            }
            if (acc.f1.f1 != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f1.f0, acc.f1.f1));
            }
            if (acc.f2.f1 != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f2.f0, acc.f2.f1));
            }
        }
    }
}
