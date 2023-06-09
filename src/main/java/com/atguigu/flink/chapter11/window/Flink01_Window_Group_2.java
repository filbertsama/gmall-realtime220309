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
public class Flink01_Window_Group_2 {
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
                new WaterSensor("sensor_1", 6001L, 60),
                new WaterSensor("sensor_1", 18000L, 60)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        tEnv.createTemporaryView("sensor", table);
        
        /*tEnv.sqlQuery("select " +
                          " id, " +
                          " tumble_start(et, interval '5' second) stt, " +
                          " tumble_end(et, interval '5' second) edt, " +
                          " sum(vc) vc_sum " +
                          "from sensor " +
                          "group by id, tumble(et, interval '5' second)")
            .execute()
            .print();*/
    
        tEnv.sqlQuery("select " +
                          " id, " +
                          " hop_start(et, interval '5' second, interval '13' second) stt, " +
                          " hop_end(et, interval '5' second, interval '13' second) edt, " +
                          " sum(vc) vc_sum " +
                          "from sensor " +
                          "group by id, hop(et, interval '5' second, interval '13' second)")
            .execute()
            .print();
        /*tEnv.sqlQuery("select " +
                          " id, " +
                          " session_start(et, interval '2' second) stt, " +
                          " session_end(et, interval '2' second) edt, " +
                          " sum(vc) vc_sum " +
                          "from sensor " +
                          "group by id, session(et, interval '2' second)")
            .execute()
            .print();*/
        
        
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