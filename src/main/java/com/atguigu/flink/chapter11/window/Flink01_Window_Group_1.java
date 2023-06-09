package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Author lzc
 * @Date 2022/8/12 8:31
 */
public class Flink01_Window_Group_1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<WaterSensor> stream = env
            .fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_2", 5000L, 50),
                new WaterSensor("sensor_1", 6001L, 60)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        
        //用tableapi的方式来实现窗口 literal 字面量  1 2  "abc"
//        GroupWindow win = Tumble.over(lit(5).second()).on($("et")).as("w");
//        GroupWindow win = Slide.over(lit(5).second()).every(lit(2).second()).on($("et")).as("w");
        GroupWindow win = Session.withGap(lit(2).second()).on($("et")).as("w");
        table
            .window(win)
            .groupBy($("id"), $("w"))
            .select($("id"), $("w").start(), $("w").end(), $("vc").sum().as("vc_sum"))
            .execute()
            .print();
        
        
    }
}
/*
select  w, id, sum(vc)  from t group by id, w


 */