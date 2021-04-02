package com.example._4_sink;

import com.example.modul.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:20 PM
 */
public class SinkTest3_ES {
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

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(
                httpHosts, new MyEsSinkFunction()).build());

        // 启动流处理程序
        env.execute();
    }

    // 实现自定义es写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {

        @Override
        public void process(SensorReading sensorReading,
                            RuntimeContext runtimeContext,
                            RequestIndexer requestIndexer) {
            // 定义写入数据source
            Map<String, String> dataSource = new HashMap<>();
            dataSource.put("sensor_id", sensorReading.getSensorId());
            dataSource.put("timestamp", sensorReading.getTimestamp().toString());
            dataSource.put("temperature", sensorReading.getTemperature().toString());

            // 创建请求，作为向es发起写入的命令
            IndexRequest indexRequest = Requests
                    .indexRequest()
                    .index("sensor")
                    .source(dataSource);

            // 发送请求
            requestIndexer.add(indexRequest);

        }
    }
}
