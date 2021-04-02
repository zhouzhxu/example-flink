package com.example._3_transform;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:17 PM
 */
public class TransformTest4_MultiStreams {
    public static void main(String[] args) throws Exception {

        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> inputDataStream = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                }
        );

        // 标识高温侧输出流
        OutputTag<SensorReading> high = new OutputTag<SensorReading>("high") {
        };
        // 标识低温侧输出流
        OutputTag<SensorReading> low = new OutputTag<SensorReading>("low") {
        };

        // 分流
        SingleOutputStreamOperator<SensorReading> process = dataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {

                        if (sensorReading.getTemperature() >= 36.5) {
                            context.output(high, sensorReading);
                        }

                        if (sensorReading.getTemperature() <= 36.0) {
                            context.output(low, sensorReading);
                        }

                        collector.collect(sensorReading);

                    }
                });

//        process.print();

        DataStream<SensorReading> highStream = process.getSideOutput(high);
        DataStream<SensorReading> lowStream = process.getSideOutput(low);

        highStream.print("high");
        lowStream.print("low");

        // 合流 connect只能合并两条流 将高温流转换为Tuple2类型与低温流合并链接之后输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highStream
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                        return new Tuple2<>(sensorReading.getSensorId(), sensorReading.getTemperature());
                    }
                });

        // 本质上还是两条流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warningStream
                .connect(lowStream);

        SingleOutputStreamOperator<Object> outputStreamOperator = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> tuple2) throws Exception {
                return new Tuple3<>(tuple2.f0, tuple2.f1, "high temperature warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getSensorId(), "normal");
            }
        });

        outputStreamOperator.print("connect -> CoMap");

        SingleOutputStreamOperator<Object> outputStreamOperator1 = connectStream.flatMap(new CoFlatMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public void flatMap1(Tuple2<String, Double> tuple2, Collector<Object> collector) throws Exception {
                collector.collect(new Tuple3<>(tuple2.f0, tuple2.f1, "high temperature warning"));
            }

            @Override
            public void flatMap2(SensorReading sensorReading, Collector<Object> collector) throws Exception {
                collector.collect(new Tuple2<>(sensorReading.getSensorId(), "normal"));
            }
        });

        outputStreamOperator1.print("connect -> CoFlatMap");

        // 合流 union可以联合多条流 但是合并的流数据类型必须一样
        DataStream<SensorReading> unionStream = highStream.union(lowStream, dataStream);
        unionStream.print("union");

        env.execute();
    }
}
