package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/2 9:25
 */
public class Flink04_Process_NoKey {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // KeyBy之前用
        // 计算所有传感器的水位和
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1L, 10),
            new WaterSensor("sensor_2", 1L, 20),
            new WaterSensor("sensor_1", 1L, 30),
            new WaterSensor("sensor_1", 1L, 40),
            new WaterSensor("sensor_2", 1L, 50)
        );
    
        stream
            .process(new ProcessFunction<WaterSensor, String>() {
            
                // 并行度是: 有几个sum?  2个
                int sum = 0;
            
                @Override
                public void processElement(WaterSensor value,  //  需要处理的元素
                                           Context ctx,  // 上下文
                                           Collector<String> out) throws Exception {
                    sum += value.getVc();
                
                    out.collect("总的水位和: " + sum);
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