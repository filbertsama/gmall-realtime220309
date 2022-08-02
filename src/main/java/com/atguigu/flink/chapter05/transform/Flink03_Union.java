package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/2 9:25
 */
public class Flink03_Union {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        DataStreamSource<Integer> s1 = env.fromElements(10, 11, 9, 20, 12);
        DataStreamSource<Integer> s2 = env.fromElements(110, 111, 19, 120, 12);
        DataStreamSource<Integer> s3 = env.fromElements(1110, 1111, 191, 1120, 112);
        
        DataStream<Integer> result = s1.union(s2, s3);
        result
            .map(new MapFunction<Integer, String>() {
                @Override
                public String map(Integer value) throws Exception {
                    
                    return value + "<>";
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
connect:  同床异梦
    1. 只能两个流连在一起
    2. 两个刘波的数据类型可以不一样, 实际情况也是大部分情况都是不同类型
   
----
union: 水乳交融
1. 可以同时多个流union在一起
2. 类型必须一致
 */