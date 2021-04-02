package com.example._7_processfunction;

import com.example.modul.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 自定义udf函数，实现高低温分流
 * @data: 2021/3/31 12:26 PM
 */
public class ProcessTest3_SideOutPutCase {
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

        // 定义一个OutputTag 用来表示侧输出流低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("low-temp") {
        };

        // 测试 KeyedProcessFunction ,  温度连续上升
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream
                .process(new HighAndLowTemperatureShunt(36.5, lowTempTag));

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }

    // 实现udf函数 测试侧输出流
    public static class HighAndLowTemperatureShunt extends ProcessFunction<SensorReading, SensorReading> {

        private Double tempThreshold;

        private OutputTag<SensorReading> lowTempTag;

        public HighAndLowTemperatureShunt(Double tempThreshold, OutputTag<SensorReading> lowTempTag) {
            this.tempThreshold = tempThreshold;
            this.lowTempTag = lowTempTag;
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {

            // 判断温度是否大于高温阈值，如果大于高温阈值，则输出到主流
            if (sensorReading.getTemperature() > tempThreshold) {
                collector.collect(sensorReading);
            } else {
                context.output(lowTempTag, sensorReading);
            }

        }
    }
}
