package com.example._5_window;

import com.example.modul.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:21 PM
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
//        DataStream<String> inputDataStream = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // 从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 9999);

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                }
        );

//        dataStream.print();

        DataStream<Tuple2<String, Integer>> aggregate = dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading sensorReading, long l) {
                                return sensorReading.getTimestamp();
                            }
                        }))
                .keyBy(SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))  // 滚动事件时间窗口,必须设置
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15))) // 滚动处理时间窗口
//                .countWindow(5) // 全局滚动计数窗口
//                .countWindow(5,2) // 全局滑动计数窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(15))) // 会话窗口
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(15))

                .aggregate(new AggregateFunction<SensorReading, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return new Tuple2<>("", 0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(SensorReading sensorReading, Tuple2<String, Integer> stringIntegerTuple2) {
                        return new Tuple2<>(sensorReading.getSensorId(), stringIntegerTuple2.f1 + 1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> stringIntegerTuple2) {
                        return stringIntegerTuple2;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> acc1) {
                        return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + acc1.f1);
                    }
                });

        //        aggregate.print();

        // 全窗口函数
        DataStream<Tuple3<String, Long, Integer>> apply = dataStream
                .keyBy(SensorReading::getSensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                // 最好使用process
                /*.process(new ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        long windowEnd = context.window().getEnd();
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(s,windowEnd,count));
                    }
                })*/
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) {
                        long windowEnd = timeWindow.getEnd();
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(s, windowEnd, count));
                    }
                });

        apply.print();

        // 启动流处理程序
        env.execute();

    }
}
