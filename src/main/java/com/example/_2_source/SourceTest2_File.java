package com.example._2_source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 12:10 PM
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> dataStreamSource = env.readTextFile("D:\\IDEAWorkSpace\\flink\\src\\main\\resources\\sensor.txt");

        // 打印输出
        dataStreamSource.print();

        // 启动流应用
        env.execute("source file test");

    }
}
