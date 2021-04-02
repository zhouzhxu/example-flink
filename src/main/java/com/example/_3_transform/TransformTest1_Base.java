package com.example._3_transform;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 基础转换
 * @data: 2021/3/31 12:12 PM
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // map 将string转换为length输出
        SingleOutputStreamOperator<Integer> map = dataStream
                .map((MapFunction<String, Integer>) s -> s.length());

        map.print("map");

        // flatMap 对数据进行拆分
        SingleOutputStreamOperator<SensorReading> flatMap = dataStream
                .flatMap(new FlatMapFunction<String, SensorReading>() {
                    @Override
                    public void flatMap(String s, Collector<SensorReading> collector) throws Exception {
                        String[] split = s.split(",");

                        SensorReading sensorReading = new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));

                        collector.collect(sensorReading);
                    }
                });

        flatMap.print("flatMap");

        // filter 过滤特定的数据
        SingleOutputStreamOperator<String> filter = dataStream
                .filter((FilterFunction<String>) s -> s.startsWith("sensor_1"));

        filter.print("filter");

        env.execute();

    }
}
