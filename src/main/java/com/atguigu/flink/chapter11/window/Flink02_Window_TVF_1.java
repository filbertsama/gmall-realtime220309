package com.atguigu.flink.chapter11.window;

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
 * @Date 2022/8/12 8:31
 */
public class Flink02_Window_TVF_1 {
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
                new WaterSensor("sensor_1", 8001L, 60),
                new WaterSensor("sensor_1", 11001L, 60),
                new WaterSensor("sensor_1", 13001L, 60),
                new WaterSensor("sensor_1", 19001L, 60),
                new WaterSensor("sensor_1", 20001L, 60),
                new WaterSensor("sensor_1", 22000L, 60),
                new WaterSensor("sensor_1", 24000L, 60),
                new WaterSensor("sensor_1", 29000L, 60)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        tEnv.createTemporaryView("sensor", table);
        
       /* tEnv
            .sqlQuery("select " +
                          " window_start,window_end, id," +
                          " sum(vc) vc_sum " +
//                          "from table( tumble( table sensor, descriptor(et), interval '5' second ) )" +
                          "from table( tumble( DATA => table sensor, TIMECOL=>descriptor(et), SIZE=> interval '5' second ) )" +
                          "group by window_start, window_end, id")
            .execute()
            .print();*/
    
        /*tEnv
            .sqlQuery("select " +
                          " window_start,window_end, id," +
                          " sum(vc) vc_sum " +
                          "from table( hop( table sensor, descriptor(et), interval '2' second ,interval '6' second ) )" +
                          "group by window_start, window_end, id")
            .execute()
            .print();*/
    
        tEnv
            .sqlQuery("select " +
                          " window_start,window_end, id," +
                          " sum(vc) vc_sum " +
                          "from table( cumulate( table sensor, descriptor(et), interval '5' second ,interval '20' second ) )" +
                          "group by window_start, window_end, id")
            .execute()
            .print();
        
        
    }
}
/*
select  w, id, sum(vc)  from t group by id, w

select
    a,b,c,
    sum(vc)
from t
group by a, b,c
union
select
    a,b,null
    sum(vc)
from t
group by a, b


select
    a,b,c,
    sum(..)

from t
group by grouping set((a,b,c), (a,b))
 */