package com.example._3_transform;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:18 PM
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {

        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> inputDataStream = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                }
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStream.map(new MyMapper());

        map.print();

        env.execute();

    }

    // 自定义富函数
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getSensorId(), getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
