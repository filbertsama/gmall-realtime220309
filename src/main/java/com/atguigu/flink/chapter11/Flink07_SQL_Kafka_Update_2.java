package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink07_SQL_Kafka_Update_2 {
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // ddl语句
        /*tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int " +
                            ")with(" +
                            " 'connector' = 'kafka', " +
                            "  'topic' = 's3', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'atguigu', " +
                            "  'scan.startup.mode' = 'earliest-offset', " +
                            "  'format' = 'json' " +
                            ")");*/
    
        tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " vc int, " +
                            " primary key(id) not enforced" +
                            ")with(" +
                            " 'connector' = 'upsert-kafka', " +
                            "  'topic' = 's3', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'key.format' = 'json', " +
                            "  'value.format' = 'json' " +
                            ")");
        
        tEnv.sqlQuery("select * from sensor").execute().print();
        
        
       
        
    }
}
