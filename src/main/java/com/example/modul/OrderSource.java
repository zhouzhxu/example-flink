package com.example.modul;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/1 10:31 PM
 */
public class OrderSource implements SourceFunction<OrderStream> {

    // 标识数据源是否仍在运行的标志
    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<OrderStream> sourceContext) throws Exception {
        // 初始化用户id
        List<String> list = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            list.add("user_" + (i + 1));
        }

        String[] products = {"手机", "电脑", "平板", "微波炉", "游戏机", "手表", "路由器"};
        Integer[] amounts = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        while (isRunning) {
            for (String s : list) {
                // 获取当前时间
                long curTime = Calendar.getInstance().getTimeInMillis();
                Thread.sleep(100);
                sourceContext.collect(new OrderStream(s,
                        products[(int) Math.floor(Math.random() * products.length)],
                        curTime,
                        amounts[(int) Math.floor(Math.random() * products.length)]));
            }
        }

    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
