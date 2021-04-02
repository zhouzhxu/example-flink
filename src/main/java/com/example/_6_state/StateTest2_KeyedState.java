package com.example._6_state;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:23 PM
 */
public class StateTest2_KeyedState {
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

        // 定义一个有状态的map操作,统计当前sensor数据个数
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultMap = dataStream
                .keyBy(SensorReading::getSensorId)
                .map(new MyKeyCountValueStateMapper());

        resultMap.print();

        env.execute();

    }

    // 自定义 RichMapFunction ValueState
    public static class MyKeyCountValueStateMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        private ValueState<Tuple2<String, Integer>> keyCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<String, Integer>> valueStateDescriptor =
                    new ValueStateDescriptor<>("key-count",
                            Types.TUPLE(Types.STRING, Types.INT));
            keyCountState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {

            Tuple2<String, Integer> value = keyCountState.value();

            if (value == null) {
                value = new Tuple2<>(sensorReading.getSensorId(), 0);
            }
            Integer count = value.f1;
            count++;
            value = new Tuple2<>(sensorReading.getSensorId(), count);
            keyCountState.update(value);
            return keyCountState.value();
        }
    }

    // 自定义 RichMapFunction MapState
    public static class MyKeyCountMapStateMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        private MapState<String,Double> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, Double.class);
            mapState=getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            mapState.put(sensorReading.getSensorId(),sensorReading.getTemperature());
            return null;
        }
    }

    // 自定义 RichMapFunction ListState
    public static class MyKeyCountListStateMapper extends RichMapFunction<SensorReading, Tuple2<String, Double>> {

        private ListState<Tuple2<String, Double>> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Tuple2<String, Double>> listStateDescriptor = new ListStateDescriptor<>("list-state", Types.TUPLE(Types.STRING, Types.DOUBLE));
            listState=getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
            return null;
        }
    }
}
