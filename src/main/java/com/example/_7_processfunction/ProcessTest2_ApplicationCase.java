package com.example._7_processfunction;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:25 PM
 */
public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env
                .socketTextStream("localhost", 9999);

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream
                .map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                });

        // 测试 KeyedProcessFunction ,  温度连续上升
        dataStream
                .keyBy((KeySelector<SensorReading, String>) sensorReading -> sensorReading.getSensorId(),
                        TypeInformation.of(String.class))
                .process(new ContinuousTemperatureRiseWarning(2))
                .print();

        env.execute();
    }

    // 实现udf函数 检测一段时间内温度连续上升报警
    public static class ContinuousTemperatureRiseWarning extends KeyedProcessFunction<String, SensorReading, Tuple2<String, String>> {

        // 定义时间间隔
        private Integer interval;

        public ContinuousTemperatureRiseWarning(Integer interval) {
            this.interval = interval;
        }

        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> tempDesc = new ValueStateDescriptor<>("last-temp", TypeInformation.of(Double.class));
            ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("timer-ts", TypeInformation.of(Long.class));
            lastTempState = getRuntimeContext().getState(tempDesc);
            timerTsState = getRuntimeContext().getState(timerDesc);
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<Tuple2<String, String>> collector) throws Exception {

            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();
            if (lastTemp != null) {
                // 如果温度上升并且没有定时器，注册10秒后的定时器开始等待
                if (sensorReading.getTemperature() > lastTemp && timerTs == null) {
                    // 计算出定时器的时间戳
                    long ts = context.timerService().currentProcessingTime() + interval * 1000L;
                    context.timerService().registerProcessingTimeTimer(ts);
                    timerTsState.update(ts);
                }
                // 如果温度下降则删除定时器
                if (sensorReading.getTemperature() < lastTemp && timerTs != null) {
                    context.timerService().deleteProcessingTimeTimer(timerTs);
                    timerTsState.clear();
                }
            }
            lastTempState.update(sensorReading.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            // 定时器触发，则输出报警信息
            out.collect(new Tuple2<>(ctx.getCurrentKey(), "Continuous Temperature Rise Warning"));
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
            timerTsState.clear();
        }
    }
}
