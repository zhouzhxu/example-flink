package com.example._5_window;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:22 PM
 */
public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 9999);

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                }
        );

        DataStream<Tuple2<String, Double>> aggregate = dataStream
                .keyBy(SensorReading::getSensorId)
                .countWindow(10, 5) // 计数窗口，窗口长度为10 滑动步长为5
                .aggregate(new MyAvgTemp());

        aggregate.print();

        env.execute();

    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return new Tuple3<>("", 0.0, 0);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
            return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> doubleIntegerTuple) {
            return new Tuple2<>(doubleIntegerTuple.f0, doubleIntegerTuple.f1 / doubleIntegerTuple.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> add(SensorReading sensorReading, Tuple3<String, Double, Integer> acc) {
            return new Tuple3<>(sensorReading.getSensorId(), acc.f1 + sensorReading.getTemperature(), acc.f2 + 1);
        }
    }
}
