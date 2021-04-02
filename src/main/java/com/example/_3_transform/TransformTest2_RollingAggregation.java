package com.example._3_transform;

import com.example.modul.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 滚动聚合
 * @data: 2021/3/31 12:17 PM
 */
public class TransformTest2_RollingAggregation {
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

        // 分组
        KeyedStream<SensorReading, String> keyedStream = dataStream
                .keyBy((KeySelector<SensorReading, String>) sensorReading -> sensorReading.getSensorId());

        dataStream
                .keyBy(SensorReading::getSensorId);

        // 滚动聚合，获取当前温度最大值，时间是第一个出现的温度时间，时间未跟随最大温度进行改变
//        DataStream<SensorReading> resultStream = keyedStream.max("temperature");

        // 滚动聚合，获取当前温度最大值，时间是最大温度时间，时间跟随最大温度进行改变
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print();

        env.execute();
    }
}
