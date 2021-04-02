package com.example.modul;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SensorSource implements SourceFunction<SensorReading> {

    // 标识数据源是否仍在运行的标志
    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        // 初始化随机数生成器
        Random random = new Random();

        // 初始化传感器ID和温度
        Map<String, Double> hashMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            hashMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }

        while (isRunning) {
            // 获取当前时间
            long curTime = Calendar.getInstance().getTimeInMillis();

//            System.currentTimeMillis(); 获取当前系统时间

            for (String sensorId : hashMap.keySet()) {
                Double newTemp = hashMap.get(sensorId) + random.nextGaussian();
                hashMap.put(sensorId, newTemp);
                sourceContext.collect(new SensorReading(sensorId, curTime, newTemp));
                Thread.sleep(10);
            }
        }

    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}