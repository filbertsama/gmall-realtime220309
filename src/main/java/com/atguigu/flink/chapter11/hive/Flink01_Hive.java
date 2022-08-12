package com.atguigu.flink.chapter11.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author lzc
 * @Date 2022/8/12 11:31
 */
public class Flink01_Hive {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        tEnv.executeSql("create table person(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int " +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/sensor.json', " +
                            " 'format' = 'json' " +
                            ")");
        
        // 1. 创建一个hivecatalog
        HiveCatalog hc = new HiveCatalog("hive", "gmall", "input/");
        
        // 2. 向表环境注册HiveCatalog
        tEnv.registerCatalog("hive", hc);
        tEnv.useCatalog("hive");
        tEnv.useDatabase("gmall");
    
        // 3. 使用hivecatalog去读取hive数据
        tEnv.sqlQuery("select " +
                          " * " +
                          "from person")
            .execute()
            .print();
        
        
        tEnv.useCatalog("default_catalog");
        tEnv.sqlQuery("select " +
                          " * " +
                          "from person")
            .execute()
            .print();
    }
}
