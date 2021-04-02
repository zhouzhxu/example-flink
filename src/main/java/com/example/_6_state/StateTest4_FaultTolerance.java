package com.example._6_state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:24 PM
 */
public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {

        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

        // 2.检查点配置
        env.enableCheckpointing(200);
        // 高级选项

        // 精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 至少一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        // 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(30000L);

        // 对齐超时时间
        env.getCheckpointConfig().setAlignmentTimeout(60000L);

        // 如果要取消任务，但是没有添加--withSavepoint，系统保留checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置检查点最大并发数量 同一个任务，前一个检查点还没存完，下一个检查点又来了，可以同时进行存储的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        // 设置检查点之间最小的间隔时间 距离上一次的Checkpoint不能小于1s
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        // 设置默认使用Checkpoint恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 设置容忍检查点可以失败的次数 默认为0
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);


        // 3.重启策略配置

        // 固定延迟重启 尝试次数 尝试重启的间隔时间
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(60)));

        // 故障率重启 5分钟内若失败了3次则认为该job失败，重试间隔为60s           重试次数   失败间隔         重试间隔
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(5), Time.seconds(60)));

        // 回滚重新启动
        env.setRestartStrategy(RestartStrategies.fallBackRestart());

        // 不重启
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 启动流处理程序
        env.execute();

    }
}
