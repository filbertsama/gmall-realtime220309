package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/2 9:25
 */
public class Flink06_Rich {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(5);
        
        // KeyBy之前用
        // 计算每个传感器的水位和
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1L, 10),
            new WaterSensor("sensor_2", 1L, 20),
            new WaterSensor("sensor_1", 1L, 30),
            new WaterSensor("sensor_1", 1L, 40),
            new WaterSensor("sensor_2", 1L, 50)
        );
        
        // 每来一个元素, 我想去查询数据库, 根据查询的结果来决定对这数据如何处理
        stream
            .map(new RichMapFunction<WaterSensor, String>() {
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 程序启动的时候, 每个并行度执行一次, 所有的初始化已经完成,环境已经可用
                    // 建立连接, 创建资源...
                    System.out.println("Flink06_Rich.open");
                }
    
                @Override
                public void close() throws Exception {
                    // 程序停止的时候每个并行度执行一次
                    // 释放资源, 关闭连接
                    System.out.println("Flink06_Rich.close");
                }
    
                @Override
                public String map(WaterSensor value) throws Exception {
                    System.out.println("Flink06_Rich.map");
                    // 建立到数据库的连接
                    
                    // 查询
                    
                    // 获取结果
                    
                    // 对象数据进行处理
                    
                    // 关闭到数据库的连接
                    
                    return value + "";
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
 
 */