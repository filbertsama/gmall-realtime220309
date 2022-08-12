package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author lzc
 * @Date 2022/8/12 8:31
 */
public class Flink03_Window_Over_1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<WaterSensor> stream = env
            .fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_1", 2000L, 30),
                new WaterSensor("sensor_1", 3000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_1", 6001L, 60)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        // over(partition by id order by et asc rows between unbounded preceding and current row)
//        OverWindow w = Over.partitionBy($("id")).orderBy($("et")).preceding(Expressions.UNBOUNDED_ROW).following(Expressions.CURRENT_ROW).as("w");
        
        // over(partition by id order by et asc rows between 1 preceding and current row)
//        OverWindow w = Over.partitionBy($("id")).orderBy($("et")).preceding(rowInterval(1L)).following(Expressions.CURRENT_ROW).as("w");
        
        
//        OverWindow w = Over.partitionBy($("id")).orderBy($("et")).preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w");
//        OverWindow w = Over.partitionBy($("id")).orderBy($("et")).preceding(lit(2).second()).following(CURRENT_RANGE).as("w");
    
        //over(partition by id order by et) => over(partition by id order by et asc range between unbounded preceding and current range)
        OverWindow w = Over.partitionBy($("id")).orderBy($("et")).as("w");
    
        table
            .window(w)
            .select($("id"), $("ts"), $("vc"), $("et"), $("vc").sum().over($("w")).as("sum_vc"))
            .execute()
            .print();
        
     
    }
}
/*
sum(vc) over(partition by id order by et asc rows between unbounded preceding and current row)


rang


 */