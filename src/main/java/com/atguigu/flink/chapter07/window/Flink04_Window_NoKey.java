package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author lzc
 * @Date 2022/8/3 14:18
 */
public class Flink04_Window_NoKey {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        env
            .socketTextStream("hadoop162", 9999)  // 一定是1
            .map(line -> {
                String[] data = line.split(",");
            
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2])
                );
            })
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum("vc").setParallelism(3)  // 窗口处理的并行度必须是: 1
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
 
}
/*
windowAll

全部: 所有的元素都会进入到同一个窗口
  比如: 0-5 所有的元素进入同一个窗口
    5-10 ...

 */