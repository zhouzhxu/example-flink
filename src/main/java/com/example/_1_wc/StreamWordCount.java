package com.example._1_wc;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从套接字端口获取数据
        DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStream
                .flatMap(new WordCount.MyFlatMap())
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) Tuple2 -> Tuple2.f0)
                .sum(1);
        sum.print();

        env.execute();
    }
}