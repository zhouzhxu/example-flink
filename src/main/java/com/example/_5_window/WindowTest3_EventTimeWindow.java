package com.example._5_window;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:22 PM
 */
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 在 Flink 1.12 中，默认的流时间特征已更改为 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10);

        // 注意：测试时设置并行度为1，方便测试
        env.setParallelism(1);

        // 从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 9999);

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream
                .map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                });

        // 乱序数据设置时间戳和watermark                                                             允许的最大乱序时间
        SingleOutputStreamOperator<SensorReading> assignTimestampsAndWatermarks = dataStream
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading sensorReading) {
//                        return sensorReading.getTimestamp();
//                    }
//                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp();
                    }
                });

        // flink1.12版本
        SingleOutputStreamOperator<SensorReading> watermarkStrategy = dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        // 提取事件里面的时间戳
                        .withTimestampAssigner((event, l) -> event.getTimestamp()));

        OutputTag<SensorReading> late = new OutputTag<>("late");

        // 基于事件时间的开窗聚合, 统计5秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = assignTimestampsAndWatermarks
                .keyBy(SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.minutes(1)) // 设置窗口的延迟关闭时间 迟到的数据还可以参与计算
                .sideOutputLateData(late) // 超出窗口延迟关闭时间外的数据，不能参与计算，如果不想舍弃，可以用侧输出拿到
                .minBy("temperature");

        minTempStream.print();
        minTempStream.getSideOutput(late).print("late");

        env.execute();
    }
}
