package com.example._1_wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {

        // 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env
                .readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\hello.txt");

        AggregateOperator<Tuple2<String, Integer>> sum = dataSource
                .flatMap(new MyFlatMap())
                .groupBy(0)
                .sum(1);

        sum.print();

//        env.execute(); 批处理不需要手动执行execute()，会自动触发execute()
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
            String[] words = s.split("\\s+");

            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }

        }
    }
}