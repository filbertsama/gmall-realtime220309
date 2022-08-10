package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink07_SQL_Kafka_Update {
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
        
        
        Table result = tEnv.sqlQuery("select id, sum(vc) vc from sensor group by id");
    
    
        tEnv.executeSql("create table abc(" +
                            " id string, " +
                            " vc int, " +
                            " primary key(id) not enforced " +  // 对主键不进行约束校验. 加主键的目的: 保证相同主键的进入到同一个分区. 主键会成为kafka的 key
                            ")with(" +
                            " 'connector' = 'upsert-kafka', " +
                            "  'topic' = 's3', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'key.format' = 'json', " +
                            "  'value.format' = 'json' " +
                            ")");
        
        result.executeInsert("abc");
        
    }
}
