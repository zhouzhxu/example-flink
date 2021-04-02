package com.example._6_state;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:24 PM
 */
public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {

        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 9999);

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream
                .map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                });

        // 定义一个有状态的flatmap操作,检测温度跳变报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultFlatMap = dataStream
                .keyBy(SensorReading::getSensorId)
                .flatMap(new TempChangeWarning(10.0));

        resultFlatMap.print();

        env.execute();

    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        // 温度跳变的阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastStateTemp;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> valueStateDescriptor = new ValueStateDescriptor<>("last-state-temp", Double.class);
            lastStateTemp = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 获取状态
            Double lastTemp = lastStateTemp.value();

            // 如果状态不为null，那就判断两次温度差值
            if (lastTemp != null) {
                Double diff = Math.abs(sensorReading.getTemperature() - lastTemp);
                if (diff >= threshold)
                    collector.collect(new Tuple3<>(sensorReading.getSensorId(), lastTemp, sensorReading.getTemperature()));
            }

            // 更新状态
            lastStateTemp.update(sensorReading.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastStateTemp.clear();
        }
    }
}
