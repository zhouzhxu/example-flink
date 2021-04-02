package com.example._4_sink;

import com.example.modul.SensorReading;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:20 PM
 */
public class SinkTest4_2_Jdbc {
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

        String sql = "insert into sensor(sensor_id, timestamp, temperature) values(?, ?, ?)";

        //1. JDBC连接MYSQL的代码很标准。
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://47.112.158.15:3306/flink?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai";
        String username = "root";
        String password = "123456";

        dataStream.addSink(JdbcSink.sink(sql, (JdbcStatementBuilder<SensorReading>) (ps, sensorReading) -> {
                    ps.setString(1, sensorReading.getSensorId());
                    ps.setLong(2, sensorReading.getTimestamp());
                    ps.setDouble(3, sensorReading.getTemperature());

                }, new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(5000)
                        .withMaxRetries(3)
                        .withBatchIntervalMs(200)
                        .build()
                , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .withUsername(username)
                        .withPassword(password)
                        .build())
        );

        // 启动流处理程序
        env.execute();
    }
}
