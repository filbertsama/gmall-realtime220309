package com.atguigu.flink.chapter11.time;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/8/10 15:58
 */
public class Flink02_Time_Event {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStream<WaterSensor> stream = env
            .fromElements(
                new WaterSensor("sensor_1", 1001L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_1", 6000L, 60)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 1. 在流转成表的时候添加  添加一个新的的字段
//        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts").rowtime(), $("vc"));
        table.printSchema();
        
        table.execute().print();
    }
}
