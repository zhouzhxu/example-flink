package com.example._4_sink;

import com.example.modul.SensorReading;
import com.example.modul.SensorSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:20 PM
 */
public class SinkTest4_1_Jdbc {
    public static void main(String[] args) throws Exception {
        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*// 从文件读取数据
        DataStream<String> inputDataStream = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // 转换
        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                }
        );*/
        DataStreamSource<SensorReading> dataStream = env.addSource(new SensorSource());

        dataStream.addSink(new MySinkMysql());

        // 启动流处理程序
        env.execute();
    }

    public static class MySinkMysql extends RichSinkFunction<SensorReading> {

        // 声明连接和预编译语句
        private Connection connection = null;
        private PreparedStatement insert = null;
        private PreparedStatement update = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager
                    .getConnection("jdbc:mysql://47.112.158.15:3306/flink?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai",
                            "root",
                            "123456");
            insert=connection.prepareStatement("insert into sensor(sensor_id, timestamp, temperature) values(?, ?, ?)");
            update=connection.prepareStatement("update sensor set timestamp = ?, temperature = ? where sensor_id = ?");
        }

        @Override
        public void close() throws Exception {
            insert.close();
            update.close();
            connection.close();
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 直接执行更新语句，如果没有更新就插入
            update.setLong(1,value.getTimestamp());
            update.setDouble(2,value.getTemperature());
            update.setString(3,value.getSensorId());
            update.execute();
            if (update.getUpdateCount()==0){
                insert.setString(1, value.getSensorId());
                insert.setLong(2, value.getTimestamp());
                insert.setDouble(3, value.getTemperature());
                insert.execute();
            }
        }
    }
}
