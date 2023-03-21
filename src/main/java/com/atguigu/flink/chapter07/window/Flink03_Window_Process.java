package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/3 14:18
 */
public class Flink03_Window_Process {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
//            .socketTextStream("hadoop162", 9999)
            .readTextFile("input/sensor.txt")
            .map(line -> {
                String[] data = line.split(",");
                
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2])
                );
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((ws, ts) -> ws.getTs())
            )
            .keyBy(WaterSensor::getId)
            .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
            //            .sum("vc")
            .reduce(
                new ReduceFunction<WaterSensor>() {
                    // 参数1: 上次聚合的结果  参数2: 本次需要参与聚合的元素
                    @Override
                    public WaterSensor reduce(WaterSensor value1,
                                              WaterSensor value2) throws Exception {
                        System.out.println("Flink03_Window_Process.reduce");

                        value1.setVc(value1.getVc() + value2.getVc());
                        return value1;
                    }
                },
                // 前面聚合函数的输出是这个窗口处理函数的输入!!!
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        // 这个集合中有且仅有一个元素: 就是前面聚合的最终结果
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        System.out.println("Flink03_Window_Process.process");
                        WaterSensor result = elements.iterator().next();
    
                        String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                        String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
                        
                        out.collect(stt + " " + edt +  " " + result);
                    }
                }
            )
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
/*
窗口内的元素进行处理, 用到窗口处理函数, 分两大类:

增量处理
    简单聚合
        sum, max, min
        maxBy minBy
    
    
    reduce
    
    
    aggregate



全量处理
    
    process


 */