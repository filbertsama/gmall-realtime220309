package com.atguigu.flink.chapter11.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/10 15:58
 */
public class Flink02_Time_Event_1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    
       
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 1. 在ddl中
        // 把bigint -> timestamp(3)
        tEnv.executeSql("create table abc(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int," +
//                            " et as to_timestamp(FROM_UNIXTIME(ts / 1000)) " +
                            " et as to_timestamp_ltz(ts, 3)," +
                            " watermark for et as et - interval '3' second " +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/sensor.json', " +
                            " 'format' = 'json' " +
                            ")");
    
        Table table = tEnv.from("abc");
        table.printSchema();
    
        tEnv.sqlQuery("select * from abc").execute().print();
      
    }
}
