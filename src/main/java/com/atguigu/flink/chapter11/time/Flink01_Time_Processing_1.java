package com.atguigu.flink.chapter11.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/10 15:58
 */
public class Flink01_Time_Processing_1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    
       
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 1. 在ddl中指定处理时间
        tEnv.executeSql("create table abc(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int," +
                            " pt as proctime() " +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/sensor.json', " +
                            " 'format' = 'json' " +
                            ")");
        
        tEnv.sqlQuery("select * from abc").execute().print();
      
    }
}
