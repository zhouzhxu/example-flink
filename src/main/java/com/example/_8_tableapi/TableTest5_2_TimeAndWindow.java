package com.example._8_tableapi;

import com.example.modul.OrderSource;
import com.example.modul.OrderStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_RANGE;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 18:14 PM
 */
public class TableTest5_2_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<OrderStream> inputStream = env
                .addSource(new OrderSource());

        /*DataStream<OrderStream> inputStream = env.fromCollection(Arrays.asList(
                new OrderStream("user_1", "beer", 1505529000L, 3), //2017-09-16 10:30:00
                new OrderStream("user_1", "beer", 1505529000L, 3), //2017-09-16 10:30:00
                new OrderStream("user_1", "rubber", 1505527800L, 2),//2017-09-16 10:10:00
                new OrderStream("user_1", "rubber", 1505527800L, 2),//2017-09-16 10:10:00
                new OrderStream("user_1", "diaper", 1505528400L, 4),//2017-09-16 10:20:00
                new OrderStream("user_1", "diaper", 1505528400L, 4)//2017-09-16 10:20:00
        ));*/

        SingleOutputStreamOperator<OrderStream> dataStream = inputStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderStream>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((e, l) -> e.getRowtime())
                );

        dataStream.print("dataStream");

        Table dataTable = tableEnv.fromDataStream(dataStream,
                $("user"),
                $("product"),
                $("rowtime").rowtime(),
                $("amount"));

        Table resultTable = dataTable
                .window(Over
                        .partitionBy($("user"), $("product"))
                        .orderBy($("rowtime"))
                        .preceding(UNBOUNDED_RANGE)
                        .as("ow")) // 注意：此处不能使用 Expression 表达式
                .select($("user"),
                        $("product"),
                        $("rowtime"),
//                        $("amount").sum0().over($("ow")).as("sumAm") // sum0()函数 如果该列没值，默认为0
                        $("amount").sum().over($("ow")).as("sumAm")); // 注意：后面使用over开窗时必须使用 Expression 表达式 Could not resolve over call 异常

        resultTable.printSchema();
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");

        env.execute();


    }
}
