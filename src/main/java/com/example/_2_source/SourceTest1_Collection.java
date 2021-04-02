package com.example._2_source;

import com.example.modul.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 集合读取数据测试
 * @data: 2021/3/31 12:09 PM
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合读取数据
        DataStream<SensorReading> fromSource = env.
                fromCollection(Arrays.asList(
                        new SensorReading("sensor_1", 1615344144L, 36.5),
                        new SensorReading("sensor_2", 1615344156L, 36.5),
                        new SensorReading("sensor_3", 1615344134L, 36.5),
                        new SensorReading("sensor_4", 1615344125L, 36.5)
                ));

        // 打印流
        fromSource.print();

        // 启动流应用
        env.execute("source Collection Test");

    }
}
