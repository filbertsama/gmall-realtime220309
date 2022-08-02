package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/8/2 9:25
 */
public class Flink05_Process_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // KeyBy之前用
        // 计算每个传感器的水位和
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1L, 10),
            new WaterSensor("sensor_2", 1L, 20),
            new WaterSensor("sensor_1", 1L, 30),
            new WaterSensor("sensor_1", 1L, 40),
            new WaterSensor("sensor_2", 1L, 50)
        );
        
        stream
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                Map<String, Integer> map = new HashMap<>();
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer sum = map.getOrDefault(ctx.getCurrentKey(), 0);
                    sum += value.getVc();
                    map.put(ctx.getCurrentKey(), sum);
;
                    out.collect(ctx.getCurrentKey() + " 的水位和: " + sum);
                        
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