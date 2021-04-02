package com.example._6_state;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
public class StateTest1_OperatorState {
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

        // 定义一个有状态的map操作,统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultMap = dataStream
                .map(new MyCountMapper());

        resultMap.print();

        env.execute();
    }

    // 自定义 MapFunction
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, CheckpointedFunction {

        // 定义一个本地变量，作为算子状态
        private Integer count = 0;

        private transient ListState<Integer> checkpointedState;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清除状态
            checkpointedState.clear();
            // 添加状态
            checkpointedState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>("count", TypeInformation.of(Integer.class));

            ListState<Integer> listState = context.getOperatorStateStore().getListState(listStateDescriptor);

            if (context.isRestored()) {
                for (Integer integer : listState.get()) {
                    count += integer;
                }
            }
        }
    }
}
