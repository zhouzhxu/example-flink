package com.example._3_transform;

import com.example.modul.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:17 PM
 */
public class TransformTest3_Reduce {
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
                .keyBy(SensorReading::getSensorId);

        /*keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading s1, SensorReading s2) throws Exception {
                return new SensorReading(s1.getSensorId(),
                        s2.getTimestamp(),
                        Math.max(s1.getTemperature(), s2.getTemperature()));
            }
        });*/

        /*keyedStream
                .reduce((s1, s2) -> new SensorReading(s1.getSensorId(),
                        s2.getTimestamp(),
                        s1.getTemperature() > s2.getTemperature() ? s1.getTemperature() : s2.getTemperature()));*/

        DataStream<SensorReading> resultStream = keyedStream
                .reduce((s1, s2) -> new SensorReading(s1.getSensorId(),
                        s2.getTimestamp(),
                        Math.max(s1.getTemperature(), s2.getTemperature())));

        resultStream.print();

        env.execute();

    }
}
