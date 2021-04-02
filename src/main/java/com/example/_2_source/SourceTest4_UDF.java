package com.example._2_source;

import com.example.modul.SensorReading;
import com.example.modul.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:11 PM
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataStream = env.addSource(new SensorSource());

        // 打印输出
        dataStream.print();

        // 启动流应用
        env.execute("source UDF test");

    }
}
