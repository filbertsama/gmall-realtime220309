package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink05_SQL_File {
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // ddl语句
        tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int " +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/sensor.txt', " +
                            " 'format' = 'csv' " +
                            ")");
        
        
        Table result = tEnv.sqlQuery("select * from sensor where id = 'sensor_1'");
        // query : string bigint int
        // sink : string int bigint
        
        tEnv.executeSql("create table abc(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int " +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/c.txt', " +
                            " 'format' = 'json' " +
                            ")");
        
        //        result.executeInsert("abc");  // 按照顺序写入的, 不校验字段名
        
        tEnv.executeSql("insert into abc select * from " + result);  // 同上
        
        
    }
}
