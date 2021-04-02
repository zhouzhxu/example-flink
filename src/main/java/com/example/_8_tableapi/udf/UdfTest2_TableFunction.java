package com.example._8_tableapi.udf;

import com.example.modul.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 表函数
 * @data: 2021/4/1 15:11 PM
 */
public class UdfTest2_TableFunction {
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

        // 注册UDTF函数
        tableEnv.createTemporarySystemFunction("splitFunction", new SplitFunction("_"));

        tableEnv.createTemporarySystemFunction("split", new Split("_"));

        // 自定义表函数 拆分sensorId 并输出(word,length)
        // table API
        sensorTable
//                .leftOuterJoinLateral(call("splitFunction",$("id")))
                .joinLateral(call("splitFunction", $("id")))
                .select($("id"),
                        $("ts"),
                        $("temp"),
                        $("word"),
                        $("length"));

        sensorTable
                .joinLateral("splitFunction(id)")
                .select("id,ts,temp,word,length");

        Table resultTable = sensorTable
                .joinLateral(call("split", $("id")).as("word","length"))
                .select($("id"),
                        $("ts"),
                        $("temp"),
                        $("word"),
                        $("length"));

        sensorTable
                .joinLateral("split(id) as (word,length)")
                .select("id,ts,temp,word,length");

//        resultTable.printSchema();
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");

        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        // 在SQL中调用注册函数
        tableEnv
                .sqlQuery("select id,ts,temp,word,length from sensor, " +
                        "LATERAL TABLE(SplitFunction(id))");

        tableEnv
                .sqlQuery("select id,ts,temp,word,length " +
                        "from sensor " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(id)) ON TRUE");

        // 重命名函数返回的字段
        tableEnv
                .sqlQuery("select id,ts,temp,newWord,newLength " +
                        "from sensor " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(id)) AS T(newWord, newLength) ON TRUE");


        tableEnv
                .sqlQuery("select id,ts,temp,word,length from sensor, " +
                        "LATERAL TABLE(Split(id)) as T(word, length)");

        tableEnv
                .sqlQuery("select id,ts,temp,word,length " +
                        "from sensor " +
                        "LEFT JOIN LATERAL TABLE(Split(id)) as T(word, length) ON TRUE");

        Table resultSqlTable = tableEnv
                .sqlQuery("select id,ts,temp,word,length " +
                        "from sensor " +
                        "LEFT JOIN LATERAL TABLE(Split(id)) AS T(word, length) ON TRUE");

        resultSqlTable.printSchema();
        tableEnv.toAppendStream(resultTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的 TableFunction
    public static class Split extends TableFunction<Tuple2<String, Integer>> {

        // 定义分隔符
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<>(s, s.length()));
            }
        }

    }

    // 实现自定义的 TableFunction
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        // 定义分隔符
        private String separator = ",";

        public SplitFunction(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(Row.of(s, s.length()));
            }
        }

    }
}
