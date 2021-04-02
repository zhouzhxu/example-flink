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
public class ProcessTest1_KeyedProcessFunction {
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

        // 测试 KeyedProcessFunction ,先对数据进行分组，然后自定义处理
        dataStream
                .keyBy((KeySelector<SensorReading, String>) sensorReading -> sensorReading.getSensorId(),
                        TypeInformation.of(String.class))
                .process(new MyProcess())
                .print();


        env.execute();

    }

    // 实现UDF函数
    public static class MyProcess extends KeyedProcessFunction<String, SensorReading, Tuple2<String, Long>> {

        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("ts-timer", TypeInformation.of(Long.class));
            timerState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {

            long processingTime = context.timerService().currentProcessingTime();

            context.timerService().registerProcessingTimeTimer(processingTime + 1000L);
            timerState.update(processingTime + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            System.out.println("定时器触发  " + timestamp);
            out.collect(new Tuple2<>(ctx.getCurrentKey(),timestamp));
        }

        @Override
        public void close() throws Exception {
            timerState.clear();
        }
    }
}
