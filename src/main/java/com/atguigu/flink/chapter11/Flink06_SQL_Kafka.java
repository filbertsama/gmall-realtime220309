package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink06_SQL_Kafka {
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
                            " 'connector' = 'kafka', " +
                            "  'topic' = 's1', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'atguigu', " +
                            "  'scan.startup.mode' = 'latest-offset', " +
                            "  'format' = 'csv' " +
                            ")");
        
        
        Table result = tEnv.sqlQuery("select * from sensor");
    
    
        tEnv.executeSql("create table abc(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int " +
                            ")with(" +
                            " 'connector' = 'kafka', " +
                            "  'topic' = 's2', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'format' = 'json' " +
                            ")");
        
        result.executeInsert("abc");
        
    }
}
