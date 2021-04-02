package com.example._8_tableapi;


import com.example.modul.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 14:41 PM
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {

        // 1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String filePath = "D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt";
        // 2.读入文件数据，得到DataStream
        // 从文件读取数据
        DataStream<String> inputDataStream = env.readTextFile(filePath);

        // 3.转换成POJO
        DataStream<SensorReading> dataStream = inputDataStream
                .map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                });

        SingleOutputStreamOperator<SensorReading> watermarkDataStream = inputDataStream
                .map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((event, l) -> event.getTimestamp()));

        // 4.将流转换成表
        // 4.1 定义处理时间特性
        Table dataTable = tableEnv.fromDataStream(dataStream,
                $("sensorId"),
                $("timestamp").as("rt"),
                $("temperature").as("temp"),
                $("pt").proctime());

        /*Table dataTable = tableEnv.fromDataStream(dataStream,
                "sensorId, timestamp as rt, temperature as temp, pt.proctime");*/

//        dataTable.printSchema();
//        tableEnv.toAppendStream(dataTable, Row.class).print();

        // 4.2 定义事件时间特性
        Table dataTable2 = tableEnv.fromDataStream(watermarkDataStream,
                $("sensorId"),
                $("timestamp").rowtime().as("rt"),
                $("temperature").as("temp"));

//        dataTable2.printSchema();
//        tableEnv.toAppendStream(dataTable2, Row.class).print();

        // 5 window 操作
        // 5.1 group window
        // table API
        Table resultTable = dataTable2
                .window(Tumble
                        .over(lit(10)
                                .second())
                        .on($("rt"))
                        .as("tw"))
                .groupBy($("sensorId"), $("tw"))
                .select($("sensorId"),
                        $("sensorId").count().as("cnt"),
                        $("temp").avg().as("avgTemp"),
                        $("tw").end());

        dataTable2.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("sensorId, tw")
                .select("sensorId, sensorId.count as cnt, temp.avg as avgTemp, tw.end ");

//        resultTable.printSchema();
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");

        tableEnv.createTemporaryView("sensor", dataTable2);
        // group SQL
        Table resultSqlTable = tableEnv.sqlQuery("select sensorId, count(sensorId) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
                "from sensor " +
                "group by sensorId, tumble(rt, interval '10' second) ");

//        resultSqlTable.printSchema();
//        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        // 5.2 over window
        // table API
        // 使用 Expression 表达式出现异常，string方式没问题
        Table overResult = dataTable2
                .window(Over
                        .partitionBy($("sensorId"))
                        .orderBy($("rt"))
                        .preceding(UNBOUNDED_RANGE)
                        .as("ow"))
                .select($("sensorId"),
                        $("rt"),
                        $("sensorId").count().over($("ow")),
                        $("temp").avg().over($("ow")));// 注意：后面使用over开窗时必须使用 Expression 表达式 Could not resolve over call 异常

//        overResult.printSchema();
//        tableEnv.toAppendStream(overResult, Row.class).print("result");

        Table overResult2 = dataTable2
                .window(Over
                        .partitionBy("sensorId")
                        .orderBy("rt")
                        .preceding("2.rows")
                        .as("ow"))
                .select("sensorId, rt, sensorId.count over ow as cnt, temp.avg over ow as avgTemp ");

//        overResult2.printSchema();
//        tableEnv.toRetractStream(overResult2, Row.class).print("2");

        // over SQL
        Table overSqlResult = tableEnv
                .sqlQuery("select sensorId, rt, count(sensorId) over ow as cnt, avg(temp) over ow as avgTemp " +
                "from sensor " +
                "window ow as (partition by sensorId order by rt rows between 2 preceding and current row)");
        overSqlResult.printSchema();
        tableEnv.toRetractStream(overSqlResult, Row.class).print("overSqlResult");

        env.execute();
    }
}
